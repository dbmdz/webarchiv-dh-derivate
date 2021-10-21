from io import BytesIO
from typing import Dict

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ByteType
from warcio.bufferedreaders import ChunkedDataReader, BufferedReader, try_brotli_init


def parse_headers(response: str) -> Dict:
    """
    Parse HTTP headers from HTTP response.
    :param response:
    :return: Dictionary of header fields and their values
    """
    try:
        headers, _ = response.split("\r\n\r\n", maxsplit=1)
    except ValueError:
        return None
    header_lines = headers.split("\n")
    parsed_headers = {}
    for line in header_lines[1:]:
        try:
            key, value = line.split(":", maxsplit=1)
            parsed_headers[key.lower().strip()] = value.lower().strip()
        except ValueError:
            continue
    return parsed_headers


def decompress(content: str, bytes: bytes) -> bytes:
    """
    Dechunk and decompress the payload of a HTTP response based on the information in the HTTP headers.
    :param content: HTTP response, including HTTP headers
    :param bytes: payload of HTTP response
    :return: dechunked and decompressed payload
    """
    try_brotli_init()
    stream = BytesIO(bytes)
    headers = parse_headers(content)
    if headers is None:
        return bytes
    try:
        encoding = headers["content-encoding"]
    except KeyError:
        encoding = None
    if (
        "transfer-encoding" in headers.keys()
        and headers["transfer-encoding"] == "chunked"
    ):
        bytes = ChunkedDataReader(stream, decomp_type=encoding).read()
    else:
        bytes = BufferedReader(stream, decomp_type=encoding).read()
    return bytes


decompress_udf = udf(decompress, ByteType())
decode_udf = udf(lambda bytes: bytes.decode("UTF-8", errors="replace"), StringType())
