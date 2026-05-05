import argparse
from pathlib import Path

from ons_inequality.clients.ons_api import ONSClient
from ons_inequality.extraction.ons_extractor import ONSExtractor


def main():
    parser = argparse.ArgumentParser(
        description="Download ONS dataset to raw data layer"
    )

    parser.add_argument("--dataset_id", required=True)
    parser.add_argument("--edition", required=True)
    parser.add_argument("--version", required=True)

    args = parser.parse_args()

    client = ONSClient()

    extractor = ONSExtractor(
        client=client,
        raw_path=Path("data/raw/ons"),
    )

    extractor.download_dataset(
        dataset_id=args.dataset_id,
        edition=args.edition,
        version=args.version, 
    )


if __name__ == "__main__":
    main()