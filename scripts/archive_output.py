import os
import zipfile
from datetime import datetime

def get_files_by_type(directory, filetype):
    """Return a list of files in the directory matching the given filetype, sorted by creation time."""
    files = [f for f in os.listdir(directory) if f.endswith(filetype)]
    files = [os.path.join(directory, f) for f in files]
    return sorted(files, key=os.path.getctime)

def zip_and_remove_files(files_to_archive, archive_dir, context_label, filetype):
    """Zip the provided files and remove originals."""
    if not files_to_archive:
        print(f"No {context_label} files to archive for type {filetype}")
        return

    os.makedirs(archive_dir, exist_ok=True)

    earliest_ts = datetime.fromtimestamp(os.path.getctime(files_to_archive[0])).strftime('%Y%m%d_%H%M%S')
    latest_ts = datetime.fromtimestamp(os.path.getctime(files_to_archive[-1])).strftime('%Y%m%d_%H%M%S')

    archive_name = f"{context_label}_output_{filetype.lstrip('.')}_{earliest_ts}_to_{latest_ts}.zip"
    archive_path = os.path.join(archive_dir, archive_name)

    with zipfile.ZipFile(archive_path, 'w') as archive_zip:
        for file in files_to_archive:
            try:
                archive_zip.write(file, os.path.basename(file))
                os.remove(file)
                print(f"Zipped and removed: {file}")
            except Exception as e:
                print(f"Failed to archive {file}: {e}")

    print(f"Archive created: {archive_path}")

def archive_output_files(output_root, filetype, retain_count):
    """Archive matching files in each subdirectory under the output root, keeping the most recent N files."""
    for subdir in os.listdir(output_root):
        sub_path = os.path.join(output_root, subdir)
        if not os.path.isdir(sub_path):
            continue

        files = get_files_by_type(sub_path, filetype)
        files_to_archive = files[:-retain_count] if len(files) > retain_count else []

        if not files_to_archive:
            continue

        archive_dir = os.path.join(sub_path, 'archive')
        context_label = os.path.basename(sub_path.rstrip(os.sep))
        zip_and_remove_files(files_to_archive, archive_dir, context_label, filetype)

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.abspath(os.path.join(current_dir, '..', 'output'))
    target_filetypes = ['.db', '.xlsx']
    retain_count = 3

    if os.path.exists(output_dir):
        for filetype in target_filetypes:
            archive_output_files(output_dir, filetype, retain_count)
    else:
        print(f"Output directory '{output_dir}' does not exist.")
