# mirrulations-fetch 

This is a command line tool that allows data scientists and researchers to efficiently download all data for a single docket from the public AWS Open Data S3 bucket [`mirrulations`](https://registry.opendata.aws/mirrulations/).

## Features
- Downloads all text and (optionally) binary data for a given docket.
- Shows live progress and ETA.
- Does **not** require AWS credentials (uses public/unsigned access).

## Requirements
- Python 3.9+

Install dependencies with:
```bash
pip install .
```

## Usage

```bash
mirrulations-fetch <docket_id> [OPTIONS]
```

### Arguments
- `<docket_id>`: The docket ID (e.g., `DEA-2024-0059`)

### Options
- `--output-folder <target>`: Target output folder (default: current directory)
- `--include-binary`: Include binary data in the download (default: off)

### Example
Download all data for docket `DEA-2024-0059` from the DEA agency into the current directory:

```bash
mirrulations-fetch DEA-2024-0059
```

Download including binary data, into a custom folder named `mydata`:

```bash
mirrulations-fetch DEA-2024-0059 --include-binary --output-folder ./mydata
```

## Output Structure
The downloaded data will be organized as follows:

```
<output-folder>/
  <docket_id>/
    raw-data/
      docket/
      documents/
      comments/
      binary-<docket_id>/   # (if --include-binary)
    derived-data/
      <all derived data folders and files>
```

## License

This project is licensed under the MIT License.
