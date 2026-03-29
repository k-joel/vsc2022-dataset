# VSC 2022 Dataset Downloader

This is a simple downloader tool for the [2023 Video Similarity Challenge](https://sites.google.com/view/vcdw2023/video-similarity-challenge) dataset.

The original codebase is available at [facebookresearch/vsc2022](https://github.com/facebookresearch/vsc2022).

## About

This project provides a downloader script to fetch the VSC 2022 dataset videos. The dataset contains query and reference videos for video similarity matching tasks.

## Usage

Run `python download.py` to download the videos. The script reads URLs from `urls/vsc_url_list.txt` and saves videos to the `videos/` directory.

## Dataset Structure

- `videos/dataset_test/` - Test set videos
- `videos/dataset_train/` - Training set videos  
- `videos/dataset_val/` - Validation set videos
- `metadata/` - Ground truth and query metadata files
- `urls/` - List of download URLs

## Original Challenge

For challenge details, evaluation code, and baseline results, please refer to the [original VSC codebase](https://github.com/facebookresearch/vsc2022).

## License

This downloader tool is provided for convenience. The VSC dataset and original codebase are released under the [MIT license](https://github.com/facebookresearch/vsc2022/blob/main/LICENSE).