import sys
import requests
from mastodon import Mastodon, StreamListener
from bs4 import BeautifulSoup
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')

API_GATEWAY_URL = "Your_Gateway_Url"

def strip_html(html):
    return BeautifulSoup(html, "html.parser").get_text()

class MyListener(StreamListener):
    def on_update(self, status):
        if status is None:
            print("Received None status, skipping...")
            return

        try:
            language = status.get('language', 'en')
            if language == 'en':
                post_id = status.get('id')
                created_at = status.get('created_at')
                created_at_str = created_at.isoformat() if isinstance(created_at, datetime) else str(created_at)
                post_content = strip_html(status.get('content', 'Not Specified'))
                post_url = status.get('url', 'Not Specified')

                user = status.get('account', {})
                if user is None:
                    user = {}
                username = user.get('username', 'Not Specified')
                display_name = user.get('display_name', 'Not Specified')
                followers_count = user.get('followers_count', 0)
                following_count = user.get('following_count', 0)
                statuses_count = user.get('statuses_count', 0)

                tags = [tag['name'] for tag in status.get('tags', [])]

                application = status.get('application', {})
                if application is None:
                    application = {}
                application_name = application.get('name', 'Unknown')

                media = status.get('media_attachments', [])
                media_type = [media_item.get('type', 'N/A') for media_item in media]

                sqs_message = {
                    "post_id": post_id,
                    "created_at": created_at_str,
                    "language": language,
                    "content": post_content,
                    "url": post_url,
                    "username": username,
                    "display_name": display_name,
                    "followers_count": followers_count,
                    "following_count": following_count,
                    "statuses_count": statuses_count,
                    "tags": tags,
                    "application": application_name,
                    "media_attachments": media_type
                }

                headers = {
                    "Content-Type": "application/json",
                    "x_api_key": "Your_key"  # Have to configure this in api gateway optional (if not created dont use this parameter)
                }

                response = requests.post(API_GATEWAY_URL, headers=headers, json=sqs_message)

                if response.status_code == 200:
                    print("Data successfully sent to API Gateway.")
                else:
                    print(f"Failed to send data. Status Code: {response.status_code}, Response: {response.text}")

        except Exception as e:
            print(f"Error processing post: {e}")

mastodon = Mastodon(
    access_token="Generate_through_your_account",
    api_base_url="https://mastodon.social"
)

# Run the stream this will keep polling for live posts and its data
mastodon.stream_public(
    listener=MyListener(),
    local=False,
    run_async=False,
    reconnect_async=True,
    reconnect_async_wait_sec=5
)

