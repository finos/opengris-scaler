"""
This example uses the Pillow library with Scaler to resize images in parallel.
"""

import os
import sys
import tempfile
from multiprocessing import cpu_count
from PIL import Image, UnidentifiedImageError
from scaler import SchedulerClusterCombo, Client


def process_image(source: str, dest: str):
    try:
        im = Image.open(source)
    except UnidentifiedImageError:
        return  # ignore non-image files

    # resize the image if it's too big
    if im.width > 1024 or im.height > 1024:
        im.thumbnail((1024, 1024))

    # this works because the workers are being run on the same machine as the client
    im.save(dest)
    im.close()

    print(f"Saved processed image into {dest}")


def main():
    script_path = os.path.dirname(os.path.abspath(__file__))

    if len(sys.argv) != 2:
        source_dir = os.path.join(script_path, "..", "images")
        print(f"Directory not provided as argument, using default: {source_dir}")
    else:
        source_dir = sys.argv[1]

    cluster = SchedulerClusterCombo(n_workers=cpu_count())

    with Client(address=cluster.get_address()) as client:
        with tempfile.TemporaryDirectory() as dest_dir:
            client.map(process_image, [
                (os.path.join(source_dir, filename), os.path.join(dest_dir, filename))
                for filename in os.listdir(source_dir)]
            )

    cluster.shutdown()


if __name__ == "__main__":
    main()
