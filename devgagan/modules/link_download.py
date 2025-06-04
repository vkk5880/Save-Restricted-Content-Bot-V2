import os
import re
import aiohttp
import asyncio
from devgagan import app
from devgagan.core.get_func import upload_media_telethondl
from pyrogram import Client, filters
from pyrogram.types import Message
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from devgagan import telethon_user_client  as gf
from devgagantools import fast_upload as fast_uploads
from datetime import datetime


async def download_file(url, file_path):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                with open(file_path, 'wb') as f:
                    while True:
                        chunk = await response.content.read(1024)
                        if not chunk:
                            break
                        f.write(chunk)
                return True
    return False

async def extract_video_links_from_html(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')
        
    video_links = []
    
    # Extract video links from the specific HTML structure
    for a_tag in soup.select("#videos .video-list a"):
        title = a_tag.text.strip()
        onclick = a_tag.get("onclick", "")
        
        # Extract URL using regex
        url_match = re.search(r"https?://[^\s'\)]+", onclick)
        if url_match:
            url = url_match.group(0)
            video_links.append({
                "title": title,
                "url": url
            })
    
    return video_links

async def upload_media_pyrogram(client, chat_id, file_path, caption, message_thread_id=None):
    try:
        ext = os.path.splitext(file_path)[1].lower()
        
        if ext in ('.mp4', '.mov', '.mkv', '.webm'):
            await client.send_video(
                chat_id=chat_id,
                video=file_path,
                caption=caption,
                reply_to_message_id=message_thread_id
            )
        else:
            await client.send_document(
                chat_id=chat_id,
                document=file_path,
                caption=caption,
                reply_to_message_id=message_thread_id
            )
        return True
    except Exception as e:
        print(f"Error uploading file: {e}")
        return False



async def upload_media_telethons(sender, target_chat_id, file, caption, topic_id):
    try:
        print("UPLOADING MEDIA TELETHON")
        # Get file metadata
        metadata = video_metadata(file)
        width, height, duration = metadata['width'], metadata['height'], metadata['duration']
        thumb_path = await screenshot(file, duration, sender)

        video_formats = {'mp4', 'mkv', 'avi', 'mov'}
        document_formats = {'pdf', 'docx', 'txt', 'epub'}
        image_formats = {'jpg', 'png', 'jpeg'}

        # Delete the edit message since we'll use our own progress
        #await edit.delete()
        progress_message = await gf.send_message(sender, "**__Uploading...__**")

        # Upload with floodwait handling
        try:
            uploaded = await fast_uploads(
                gf, file,
                reply=progress_message,
                name=None,
                progress_bar_function=lambda done, total: progress_callback(done, total, sender)
            )
        except FloodWaitError as fw:
            await progress_message.edit(f"⏳ FloodWait: Sleeping for {fw.seconds} seconds...")
            await asyncio.sleep(fw.seconds)
            # Retry after floodwait
            uploaded = await fast_upload(
                gf, file,
                reply=progress_message,
                name=None,
                progress_bar_function=lambda done, total: progress_callback(done, total, sender)
            )

        await progress_message.delete()

        # Prepare attributes based on file type
        attributes = []
        if file.split('.')[-1].lower() in video_formats:
            attributes.append(DocumentAttributeVideo(
                duration=duration,
                w=width,
                h=height,
                supports_streaming=True
            ))

        # Send to target chat
        await gf.send_file(
            target_chat_id,
            uploaded,
            caption=caption,
            attributes=attributes,
            reply_to=topic_id,
            thumb=thumb_path
        )

        # Send to log group
        await gf.send_file(
            LOG_GROUP,
            uploaded,
            caption=caption,
            attributes=attributes,
            thumb=thumb_path
        )
        return True

    except Exception as e:
        await gf.send_message(LOG_GROUP, f"**Upload Failed:** {str(e)}")
        print(f"Error during media upload: {e}")
        raise  # Re-raise the exception for higher level handling
        return False

    finally:
        # Cleanup
        if thumb_path and os.path.exists(thumb_path):
            os.remove(thumb_path)
        if 'progress_message' in locals():
            try:
                await progress_message.delete()
            except:
                pass
        #gc.collect()
        #return False




# Helper function to extract links and titles from file
def extract_links_from_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Pattern to match both formats:
    # Title:https://url
    # or just https://url
    pattern = r'(?:([^:\n]+):)?(https?://[^\s]+)'
    matches = re.findall(pattern, content)
    
    entries = []
    for title, url in matches:
        # Clean up title if it exists
        if title:
            title = title.strip()
        entries.append({'title': title, 'url': url})
    
    return entries

# Helper function to download files
async def download_mufile(url, file_path):
    try:
        # Use appropriate download method based on file type
        if url.endswith('.m3u8'):
            # For HLS streams
            cmd = [
                "ffmpeg",
                "-i", url,
                "-c", "copy",
                "-bsf:a", "aac_adtstoasc",
                file_path
            ]
            proc = await asyncio.create_subprocess_exec(*cmd)
            await proc.wait()
            return proc.returncode == 0
        else:
            # For regular files
            if not await download_file(url, dl_file_path):
                raise Exception("Download failed")
                return False
            return True
    except Exception as e:
        print(f"Download error: {e}")
        return False

# Format message as requested
def format_entry(entry, index, common_title=None):
    title = entry['title'] or common_title or "Untitled"
    url = entry['url']
    
    # Extract date from URL if available (looking for patterns like /2024-08-09-)
    date_match = re.search(r'/(\d{4}-\d{2}-\d{2})-', url)
    date = date_match.group(1) if date_match else datetime.now().strftime("%Y-%m-%d")
    
    # Determine if it's a video or document
    if url.endswith('.pdf'):
        file_type = "Document"
    else:
        file_type = "Video"
    
    return (
        f"✯ ━━━━❀°𝑳𝒊𝒏𝒌 𝑰𝒅: {index:02d}°❀ ━━━━ ✯\n"
        f"╭━━━━━━━━━━━━━ ❀° ━━━╮\n"
        f"┣⪼{'𝑽𝒊𝒅𝒆𝒐' if file_type == 'Video' else '𝑫𝒐𝒄𝒖𝒎𝒆𝒏𝒕'} 𝑻𝒊𝒕𝒍𝒆 : {title}\n\n"
        f"✨𝑩𝒂𝒕𝒄𝒉 𝑵𝒂𝒎𝒆: {common_title or 'No Batch'}\n"
        f"📅 𝑫𝒂𝒕𝒆: {date}\n"
        f"🔗 𝑼𝑹𝑳: {url}\n"
        f"╰━━━━━━━━━━━━━ ❀° ━━━╯\n"
    )

@app.on_message(filters.command("batchtxt") & filters.private)
async def batch_download_command(client, message: Message):
    # Ask user to send the file
    status_msg = await message.reply_text("Please send me the HTML or text file containing links.")
    
    try:
        # Wait for the user to send a document
        file_message = await client.ask(
            message.chat.id,
            "Please upload the file now.",
            filters=filters.document,
            timeout=180
        )
    except asyncio.TimeoutError:
        await status_msg.edit_text("Timed out waiting for the file. Please try again.")
        return
    
    # Check if the file is HTML or text
    file_name = file_message.document.file_name.lower()
    if not (file_name.endswith('.html') or file_name.endswith('.txt')):
        await status_msg.edit_text("Please provide an HTML or text file.")
        return
    
    # Download the file
    await status_msg.edit_text("Downloading your file...")
    file_path = await file_message.download(file_name="links_file")
    
    # Extract links
    await status_msg.edit_text("Extracting links from file...")
    entries = extract_links_from_file(file_path)
    
    if not entries:
        await status_msg.edit_text("No links found in the file.")
        os.remove(file_path)
        return
    
    # Ask for common title if no titles found
    common_title = None
    if not any(entry['title'] for entry in entries):
        try:
            title_msg = await client.ask(
                message.chat.id,
                "No titles found in the file. Please enter a common title for all links:",
                timeout=60
            )
            common_title = title_msg.text
        except asyncio.TimeoutError:
            common_title = "Untitled"
    
    # Process links one by one
    success_count = 0
    failed_entries = []
    
    for i, entry in enumerate(entries, 1):
        try:
            # Format and send the message
            formatted_msg = format_entry(entry, i, common_title)
            #await message.reply_text(formatted_msg)
            
            # Download the file
            url = entry['url']
            title = entry['title'] or common_title or f"File_{i}"
            
            # Create downloads directory if it doesn't exist
            os.makedirs("downloads", exist_ok=True)
            
            # Generate safe filename
            safe_title = "".join(c if c.isalnum() else "_" for c in title)[:100]
            ext = '.mp4' if not url.endswith('.pdf') else '.pdf'
            dl_file_path = f"downloads/{message.from_user.id}_{i}_{safe_title}{ext}"
            
            await status_msg.edit_text(f"Downloading {i}/{len(entries)}: {title}")

            if await download_mufile(url, dl_file_path):
                # Upload to Telegram
                await status_msg.edit_text(f"Uploading {i}/{len(entries)}: {title}")
                
                topic_id = None
                if file_message.reply_to_message and file_message.reply_to_message.forum_topic_created:
                    topic_id = file_message.reply_to_message.message_thread_id

                if await upload_media_telethondl(message.chat.id, message.chat.id, dl_file_path, title, topic_id):
                    success_count += 1
                else:
                    failed_entries.append(f"{title} - {url}")
            
                if os.path.exists(dl_file_path):
                    os.remove(dl_file_path)
                
            
        except Exception as e:
            print(f"Error processing entry {i}: {e}")
            failed_entries.append(f"{entry.get('title', f'Entry {i}')} - {entry.get('url', '')}")
    
    # Clean up
    os.remove(file_path)
    
    # Send final status
    result_text = (
        f"Processed {len(entries)} links.\n"
        f"✅ Success: {success_count}\n"
        f"❌ Failed: {len(failed_entries)}"
    )
    
    if failed_entries:
        result_text += "\n\nFailed entries:\n" + "\n".join(failed_entries[:5])
        if len(failed_entries) > 5:
            result_text += f"\n...and {len(failed_entries)-5} more"
    
    await status_msg.edit_text(result_text)






@app.on_message(filters.command("batchhtml") & filters.private)
async def batch_download_command(_, message: Message):
    # Ask user to send the HTML file
    status_msg = await message.reply_text("Please send me the HTML file within the next 3 minutes.")
    
    try:
        # Wait for the user to send a document within 3 minutes (180 seconds)
        file_message = await message.chat.ask(
            filters.document,
            timeout=180,
            user_id=message.from_user.id
        )
    except asyncio.TimeoutError:
        await status_msg.edit_text("Timed out waiting for the file. Please try again.")
        return
    
    # Check if the received file is an HTML file
    file_name = file_message.document.file_name.lower()
    if not file_name.endswith('.html'):
        await status_msg.edit_text("Please provide an HTML file.")
        return
    
    await status_msg.edit_text("Downloading your file...")
    file_path = await file_message.download(file_name="links_file.html")
    
    await status_msg.edit_text("Extracting video links from file...")
    video_entries = await extract_video_links_from_html(file_path)
    
    if not video_entries:
        await status_msg.edit_text("No video links found in the HTML file.")
        os.remove(file_path)
        return
    
    await status_msg.edit_text(f"Found {len(video_entries)} video lectures. Starting download...")
    
    success_count = 0
    failed_entries = []
    
    for i, entry in enumerate(video_entries, 1):
        try:
            title = entry["title"]
            url = entry["url"]
            
            title_msg = await message.reply_text(f"Downloading: {title}\nURL: {url}")
            
            # Create downloads directory if it doesn't exist
            os.makedirs("downloads", exist_ok=True)
            
            # Generate filename from title (sanitize it)
            safe_title = "".join(c if c.isalnum() else "_" for c in title)[:100]
            dl_file_path = f"downloads/{message.from_user.id}_{i}_{safe_title}.mp4"
            
            await status_msg.edit_text(f"Downloading {i}/{len(video_entries)}: {title}")
            
            # Use ffmpeg for HLS streams if available
            if url.endswith('.m3u8'):
                try:
                    cmd = [
                        "ffmpeg",
                        "-i", url,
                        "-c", "copy",
                        "-bsf:a", "aac_adtstoasc",
                        dl_file_path
                    ]
                    proc = await asyncio.create_subprocess_exec(*cmd)
                    await proc.wait()
                    
                    if proc.returncode != 0:
                        raise Exception("FFmpeg failed")
                except Exception as e:
                    print(f"FFmpeg error: {e}")
                    if await download_file(url, dl_file_path):
                        pass  # Fallback succeeded
                    else:
                        raise
            else:
                if not await download_file(url, dl_file_path):
                    raise Exception("Download failed")
            
            # Upload to Telegram
            await status_msg.edit_text(f"Uploading {i}/{len(video_entries)}: {title}")
            
            topic_id = None
            if file_message.reply_to_message and file_message.reply_to_message.forum_topic_created:
                topic_id = file_message.reply_to_message.message_thread_id
            
            if await upload_media_telethondl(
                message.chat.id,
                message.chat.id,
                dl_file_path,
                title,
                topic_id
            ):
                success_count += 1
            else:
                failed_entries.append(f"{title} - {url}")
            
            # Clean up
            if os.path.exists(dl_file_path):
                os.remove(dl_file_path)
            
            await title_msg.delete()
            
        except Exception as e:
            print(f"Error processing {entry.get('title', '')}: {e}")
            failed_entries.append(f"{entry.get('title', 'Unknown')} - {entry.get('url', '')}")
    
    # Clean up
    os.remove(file_path)
    
    # Send final status
    result_text = f"Processed {len(video_entries)} video lectures.\nSuccess: {success_count}\nFailed: {len(failed_entries)}"
    if failed_entries:
        result_text += "\n\nFailed lectures:\n" + "\n".join(failed_entries[:5])  # Show first 5 failed
        if len(failed_entries) > 5:
            result_text += f"\n...and {len(failed_entries)-5} more"
    
    await status_msg.edit_text(result_text)








@app.on_message(filters.command("batchdl") & filters.private)
async def batch_download_command(_, message: Message):
    if not message.reply_to_message or not message.reply_to_message.document:
        await message.reply_text("Please reply to an HTML file with the /batchdl command.")
        return
    
    file_name = message.reply_to_message.document.file_name.lower()
    if not file_name.endswith('.html'):
        await message.reply_text("Please provide an HTML file.")
        return
    
    status_msg = await message.reply_text("Downloading your file...")
    file_path = await message.reply_to_message.download(file_name="links_file.html")
    
    await status_msg.edit_text("Extracting video links from file...")
    video_entries = await extract_video_links_from_html(file_path)
    
    if not video_entries:
        await status_msg.edit_text("No video links found in the HTML file.")
        os.remove(file_path)
        return
    
    await status_msg.edit_text(f"Found {len(video_entries)} video lectures. Starting download...")
    
    success_count = 0
    failed_entries = []
    
    for i, entry in enumerate(video_entries, 1):
        try:
            title = entry["title"]
            url = entry["url"]
            
            title_msg = await message.reply_text(f"Downloading: {title}\nURL: {url}")
            
            # Create downloads directory if it doesn't exist
            os.makedirs("downloads", exist_ok=True)
            
            # Generate filename from title (sanitize it)
            safe_title = "".join(c if c.isalnum() else "_" for c in title)[:100]
            dl_file_path = f"downloads/{message.from_user.id}_{i}_{safe_title}.mp4"
            
            await status_msg.edit_text(f"Downloading {i}/{len(video_entries)}: {title}")
            
            # Use ffmpeg for HLS streams if available
            if url.endswith('.m3u8'):
                try:
                    cmd = [
                        "ffmpeg",
                        "-i", url,
                        "-c", "copy",
                        "-bsf:a", "aac_adtstoasc",
                        dl_file_path
                    ]
                    proc = await asyncio.create_subprocess_exec(*cmd)
                    await proc.wait()
                    
                    if proc.returncode != 0:
                        raise Exception("FFmpeg failed")
                except Exception as e:
                    print(f"FFmpeg error: {e}")
                    if await download_file(url, dl_file_path):
                        pass  # Fallback succeeded
                    else:
                        raise
            else:
                if not await download_file(url, dl_file_path):
                    raise Exception("Download failed")
            
            # Upload to Telegram
            await status_msg.edit_text(f"Uploading {i}/{len(video_entries)}: {title}")
            
            topic_id = None
            if message.reply_to_message and message.reply_to_message.forum_topic_created:
                topic_id = message.reply_to_message.message_thread_id
            
            if await upload_media_telethondl(
                message.chat.id,
                message.chat.id,
                dl_file_path,
                title,
                topic_id
            ):
                success_count += 1
            else:
                failed_entries.append(f"{title} - {url}")
            
            # Clean up
            if os.path.exists(dl_file_path):
                os.remove(dl_file_path)
            
            await title_msg.delete()
            
        except Exception as e:
            print(f"Error processing {entry.get('title', '')}: {e}")
            failed_entries.append(f"{entry.get('title', 'Unknown')} - {entry.get('url', '')}")
    
    # Clean up
    os.remove(file_path)
    
    # Send final status
    result_text = f"Processed {len(video_entries)} video lectures.\nSuccess: {success_count}\nFailed: {len(failed_entries)}"
    if failed_entries:
        result_text += "\n\nFailed lectures:\n" + "\n".join(failed_entries[:5])  # Show first 5 failed
        if len(failed_entries) > 5:
            result_text += f"\n...and {len(failed_entries)-5} more"
    
    await status_msg.edit_text(result_text)
