#!/usr/bin/env python3
"""
Script to parse vsc_url_list.txt and download CSV files.
Usage: python download_csv.py [--dl-csv]
"""

import argparse
import os
import re
import requests
from pathlib import Path
from urllib.parse import urlparse
from tqdm import tqdm


def parse_url_list(file_path):
    """
    Parse the URL list file and extract all URLs.

    Args:
        file_path: Path to the vsc_url_list.txt file

    Returns:
        List of all URLs found in the file
    """
    urls = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                url = line.strip()
                if url:  # Skip empty lines
                    urls.append(url)
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return []
    except Exception as e:
        print(f"Error reading file: {e}")
        return []

    return urls


def build_video_url_map(urls):
    """
    Build a hash table mapping video keys to URLs from the URL list.

    Args:
        urls: List of all URLs

    Returns:
        Dictionary mapping video keys (e.g., "queries/Q300001") to URLs
    """
    video_url_map = {}

    for url in urls:
        if url.lower().endswith('.mp4'):
            # Parse the URL to extract the key
            # Example URL: https://dl.fbaipublicfiles.com/video_similarity_challenge/46ef53734a4/dataset_test/queries/Q300001.mp4
            parsed_url = urlparse(url)
            path_parts = parsed_url.path.strip('/').split('/')
            if len(path_parts) >= 3:
                # Extract the key as "parent_directory/video_name"
                # e.g., "queries/Q300001" from "/video_similarity_challenge/46ef53734a4/dataset_test/queries/Q300001.mp4"
                parent_dir = path_parts[-2]  # queries or refs
                video_name = path_parts[-1].replace('.mp4', '')  # Q300001
                key = f"{parent_dir}/{video_name}"
                video_url_map[key] = url

    return video_url_map


def download_file(url, output_path_or_dir):
    """
    Download a file from a URL to the specified path or directory.

    Args:
        url: URL to download
        output_path_or_dir: Either a full output path or directory to save the file

    Returns:
        True if successful, False otherwise
    """
    try:
        # Determine if output_path_or_dir is a directory or full path
        if os.path.isdir(output_path_or_dir) or not os.path.splitext(output_path_or_dir)[1]:
            # It's a directory - construct full path from URL
            output_dir = output_path_or_dir

            # Parse URL to get path components
            parsed_url = urlparse(url)
            url_path = parsed_url.path

            # Get filename from path
            filename = os.path.basename(url_path)

            if not filename:
                print(f"Warning: Could not determine filename for {url}")
                return False

            # Get parent directory from URL path (e.g., "dataset_test" from "/video_similarity_challenge/46ef53734a4/dataset_test/queries_metadata.csv")
            path_parts = url_path.strip('/').split('/')
            if len(path_parts) >= 2:
                # Use the second-to-last part as the parent directory
                parent_dir = path_parts[-2]
                output_path = os.path.join(output_dir, parent_dir, filename)
            else:
                # If no parent directory in URL, save directly to output_dir
                output_path = os.path.join(output_dir, filename)
        else:
            # It's a full path
            output_path = output_path_or_dir

        # Check if file already exists
        if os.path.exists(output_path):
            print(f"File already exists: {os.path.basename(output_path)}")
            return True

        # Create parent directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Download the file
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # Get file size for progress bar
        total_size = int(response.headers.get('content-length', 0))

        with open(output_path, 'wb') as f:
            with tqdm(total=total_size, unit='B', unit_scale=True, desc=os.path.basename(output_path)) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))

        print(f"Downloaded: {os.path.basename(output_path)}")
        return True

    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error downloading {url}: {e}")
        return False


def download_videos(num_videos, dataset, video_url_map):
    """
    Download video pairs from the ground truth CSV file using the URL map.

    Args:
        num_videos: Number of video pairs to download
        dataset: Dataset name (test/train/val)
        video_url_map: Dictionary mapping video keys to URLs

    Returns:
        True if successful, False otherwise
    """
    try:
        # Construct the path to the ground truth CSV file
        csv_path = os.path.join(
            'metadata', f'dataset_{dataset}', f'{dataset}_ground_truth.csv')

        if not os.path.exists(csv_path):
            print(f"Error: Ground truth file not found at {csv_path}")
            return False

        # Read the CSV file
        import csv
        rows = []
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)

        if not rows:
            print(f"Error: No data found in {csv_path}")
            return False

        # Limit to the requested number of videos
        rows_to_download = rows[:num_videos]
        print(
            f"Downloading {len(rows_to_download)} video pairs from {dataset} dataset...")

        # Create the base videos directory
        videos_dir = 'videos'
        os.makedirs(videos_dir, exist_ok=True)

        # Download each video pair
        successful = 0
        failed = 0

        for idx, row in enumerate(rows_to_download, 1):
            query_id = row['query_id']
            ref_id = row['ref_id']

            # Look up URLs from the video URL map
            query_key = f"queries/{query_id}"
            ref_key = f"refs/{ref_id}"

            query_url = video_url_map.get(query_key)
            ref_url = video_url_map.get(ref_key)

            if not query_url:
                print(f"Warning: URL not found for query video {query_id}")
                failed += 1
                continue
            if not ref_url:
                print(f"Warning: URL not found for ref video {ref_id}")
                failed += 1
                continue

            # Create directory for this row
            row_dir = os.path.join(videos_dir, f'dataset_{dataset}', str(idx))
            os.makedirs(row_dir, exist_ok=True)

            # Download query video
            query_output = os.path.join(row_dir, f"{query_id}.mp4")
            print(
                f"Downloading query video {idx}/{len(rows_to_download)}: {query_id}")
            if download_file(query_url, query_output):
                successful += 1
            else:
                failed += 1

            # Download ref video
            ref_output = os.path.join(row_dir, f"{ref_id}.mp4")
            print(
                f"Downloading ref video {idx}/{len(rows_to_download)}: {ref_id}")
            if download_file(ref_url, ref_output):
                successful += 1
            else:
                failed += 1

        # Print summary
        print(f"\nVideo download complete!")
        print(f"  Successful: {successful}")
        print(f"  Failed: {failed}")
        print(f"  Total: {len(rows_to_download) * 2}")
        return True

    except Exception as e:
        print(f"Error downloading videos: {e}")
        return False


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Parse vsc_url_list.txt and download CSV files'
    )
    parser.add_argument(
        '--dl-csv',
        action='store_true',
        help='Download all CSV files to the metadata folder'
    )
    parser.add_argument(
        '--file',
        type=str,
        default='urls/vsc_url_list.txt',
        help='Path to the URL list file (default: urls/vsc_url_list.txt)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='metadata',
        help='Output directory for downloaded CSV files (default: metadata)'
    )
    parser.add_argument(
        '--dataset',
        type=str,
        choices=['test', 'train', 'val'],
        help='Specify the dataset (test/train/val)'
    )
    parser.add_argument(
        '--dl-vid',
        type=int,
        help='Number of video pairs to download'
    )

    args = parser.parse_args()

    # Parse the URL list
    print(f"Parsing URL list from: {args.file}")
    urls = parse_url_list(args.file)

    if not urls:
        print("No URLs found in the file.")
        return

    print(f"Total URLs found: {len(urls)}")

    # Build video URL map
    video_url_map = build_video_url_map(urls)
    print(f"Video URLs found: {len(video_url_map)}")

    # Filter CSV URLs and remove duplicates
    csv_urls = [url for url in urls if url.lower().endswith('.csv')]
    seen = set()
    csv_urls = [url for url in csv_urls if not (url in seen or seen.add(url))]
    print(f"Unique CSV files found: {len(csv_urls)}")

    # Display CSV URLs
    if csv_urls:
        print("\nCSV files found:")
        for i, url in enumerate(csv_urls, 1):
            print(f"  {i}. {url}")
    else:
        print("\nNo CSV files found in the URL list.")
        return

    # Download CSV files if --dl-csv flag is provided
    if args.dl_csv:
        print(f"\nDownloading CSV files to: {args.output}")

        # Create output directory if it doesn't exist
        os.makedirs(args.output, exist_ok=True)

        # Download each CSV file
        successful = 0
        failed = 0

        for url in csv_urls:
            if download_file(url, args.output):
                successful += 1
            else:
                failed += 1

        # Print summary
        print(f"\nDownload complete!")
        print(f"  Successful: {successful}")
        print(f"  Failed: {failed}")
        print(f"  Total: {len(csv_urls)}")
    elif args.dl_vid:
        # Download videos if --dl-vid flag is provided
        if not args.dataset:
            print("Error: --dataset is required when using --dl-vid")
            print("Usage: python download.py --dl-vid 10 --dataset test")
            return

        download_videos(args.dl_vid, args.dataset, video_url_map)
    else:
        print("\nTo download these CSV files, run with the --dl-csv flag:")
        print(f"  python {os.path.basename(__file__)} --dl-csv")
        print("\nTo download videos, run with --dl-vid and --dataset flags:")
        print(
            f"  python {os.path.basename(__file__)} --dl-vid 10 --dataset test")


if __name__ == '__main__':
    main()
