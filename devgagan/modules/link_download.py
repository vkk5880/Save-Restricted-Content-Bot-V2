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



# Helper function to extract links and titles from file
def extract_links_from_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Pattern to match both formats:
    # Title:https://url
    # or just https://url
    #pattern = r'(?:([^:\n]+):)?(https?://[^\s]+)'
    #pattern = r'(?:(.+?):)?(https?://[^\s]+)'
    pattern = r'^(?:(.*?)\s*:\s*)?(https?://[^\s]+)$'
    matches = re.findall(pattern, content, re.MULTILINE)
    
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
            if not await download_file(url, file_path):
                raise Exception("Download failed")
                return False
            return True
    except Exception as e:
        print(f"Download error: {e}")
        return False

# Format message as requested
def format_entry(entry, index, common_title=None):
    title = entry['title'] or "Untitled"
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
    common_title = os.path.splitext(file_name)[0]
    """if not any(entry['title'] for entry in entries):
        try:
            title_msg = await client.ask(
                message.chat.id,
                "No titles found in the file. Please enter a common title for all links:",
                timeout=60
            )
            common_title = title_msg.text
        except asyncio.TimeoutError:
            common_title = "Untitled"
    """
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

            path_without_query = url.split('?')[0]
            filename, raw_ext = os.path.splitext(path_without_query)
            ext = raw_ext.lower()
            #ext = '.mp4' if not url.endswith('.pdf') else '.pdf'
            dl_file_path = f"downloads/{i}_{title}{ext}"
            
            await status_msg.edit_text(f"Downloading {i}/{len(entries)}: {title}")

            if await download_mufile(url, dl_file_path):
                # Upload to Telegram
                await status_msg.edit_text(f"Uploading {i}/{len(entries)}: {title}")
                
                topic_id = None
                if file_message.reply_to_message and file_message.reply_to_message.forum_topic_created:
                    topic_id = file_message.reply_to_message.message_thread_id

                if await upload_media_telethondl(message.chat.id, message.chat.id, dl_file_path, formatted_msg, topic_id):
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






import tempfile

@app.on_message(filters.command("htmltotxt") & filters.private)
async def html_to_text_command(client, message: Message):
    # Ask user to send the HTML file
    status_msg = await message.reply_text("Please send me the HTML file containing links.")
    
    try:
        # Wait for the user to send a document
        file_message = await client.ask(
            message.chat.id,
            "Please upload the HTML file now.",
            filters=filters.document,
            timeout=180
        )
    except asyncio.TimeoutError:
        await status_msg.edit_text("Timed out waiting for the file. Please try again.")
        return
    
    # Check if the file is HTML
    file_name = file_message.document.file_name.lower()
    if not file_name.endswith('.html'):
        await status_msg.edit_text("Please provide an HTML file.")
        return
    
    # Download the file
    await status_msg.edit_text("Downloading your file...")
    file_path = await file_message.download(file_name="temp_html_file.html")
    
    # Extract links and titles
    await status_msg.edit_text("Extracting links and titles...")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Pattern to match both regular links and onclick links
        pattern = r'<a\s+(?:[^>]*?\s+)?(?:href="([^"]*)"|onclick="playVideo\(\'([^\']*)\'\))[^>]*>(.*?)<\/a>'
        matches = re.findall(pattern, html_content, re.DOTALL)
        
        extracted_links = []
        
        for match in matches:
            # Get URL from either href or onclick
            url = match[0] if match[0] else match[1]
            title = match[2].strip()
            
            # Clean up title - remove HTML tags and extra whitespace
            title = re.sub(r'<[^>]+>', '', title)
            title = re.sub(r'\s+', ' ', title).strip()
            
            if url and title:
                extracted_links.append(f"{title}: {url}")
        
        if not extracted_links:
            await status_msg.edit_text("No links found in the HTML file.")
            os.remove(file_path)
            return

        txt_file_name = os.path.splitext(file_name)[0] + ".txt"
        with open(txt_file_name, "w", encoding="utf-8") as txt_file:
            txt_file.write("\n\n".join(extracted_links))
            
        # Send the file to the user
        await message.reply_document(txt_file_name, caption=f"Successfully extracted {len(extracted_links)} links.")
        await status_msg.delete()
        
        """# Combine all links into a text message
        result_text = "Extracted Links:\n\n" + "\n\n".join(extracted_links)
        
        # Split long messages to avoid Telegram's message length limit
        max_length = 4000
        if len(result_text) > max_length:
            parts = [result_text[i:i+max_length] for i in range(0, len(result_text), max_length)]
            for part in parts:
                await message.reply_text(part)
                await asyncio.sleep(1)  # Avoid flooding
        else:
            await message.reply_text(result_text)
        
        await status_msg.edit_text(f"Successfully extracted {len(extracted_links)} links!")"""
    
    except Exception as e:
        await message.reply_text(f"Error processing file: {str(e)}")
    finally:
        # Clean up
        if os.path.exists(txt_file_name):
            os.remove(txt_file_name)
        if os.path.exists(file_path):
            os.remove(file_path)


# Example usage:
# User sends: /htmltotxt
# Bot replies: "Please send me the HTML file containing links."
# User uploads HTML file
# Bot processes and replies with all extracted links in format:
# Title: URL
# Title: URL
# ...
