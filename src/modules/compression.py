from src.config import print_log


def compress_index(index_name, index_file_path, index_lexicon_path):
    # compression of index file
    print_log("compressing index file", priority=1)


def to_unary(n):
    # Unary encoding: n-1 ones followed by a final zero
    return '1' * (n - 1) + '0'


def to_gamma(n):
    # Compute the binary representation of n
    binary_repr = bin(n)[2:]  # Binary representation without the '0b' prefix

    # First part: Unary encoding of the length of the binary representation
    length = len(binary_repr)
    unary_length = to_unary(length)

    # Second part: Offset, i.e., the binary representation without the most significant bit
    offset = binary_repr[1:]

    # Combine the two parts
    return unary_length + offset


def bit_stream_to_bytes(bit_stream):
    # Pad the bit stream so that its length is a multiple of 8
    padding_length = (8 - len(bit_stream) % 8) % 8
    bit_stream = bit_stream + '1' * padding_length

    # Convert each 8-bit chunk into a byte
    byte_array = bytearray()
    for i in range(0, len(bit_stream), 8):
        byte_chunk = bit_stream[i:i + 8]
        byte_array.append(int(byte_chunk, 2))

    return bytes(byte_array)


def decode_posting_list(compressed_bytes, compression=False, encoding_type="unary"):
    '''
    The decode_posting_list function first reads the binary data and converts it to a string of bits.
    :param compressed_bytes:
    :param encoding_type:
    :return: list_doc_id
    '''
    if compression:
        # Convert the bytes back into a bit string
        bit_stream = ''.join(f'{byte:08b}' for byte in compressed_bytes)
        print(bit_stream)

        # Step 1: Decode the number of doc IDs
        if encoding_type == "unary":
            num_docs, remaining_bit_stream = decode_unary(bit_stream, limit=1)
            print(num_docs)
            print(remaining_bit_stream)
        elif encoding_type == "gamma":
            num_docs, remaining_bit_stream = decode_gamma(bit_stream, limit=1)

        num_docs = num_docs[0]  # We expect a single value for num_docs

        # Step 2: Decode the doc IDs
        if encoding_type == "unary":
            decoded_doc_ids, remaining_bit_stream = decode_unary(remaining_bit_stream, limit=num_docs)
            decoded_freqs, _ = decode_unary(remaining_bit_stream, limit=num_docs)
        elif encoding_type == "gamma":
            decoded_doc_ids, remaining_bit_stream = decode_gamma(remaining_bit_stream, limit=num_docs)
            decoded_freqs, _ = decode_gamma(remaining_bit_stream, limit=num_docs)
    else:
        posting_list = compressed_bytes.split()
        decoded_doc_ids = map(int, posting_list[0].split(","))
        decoded_freqs = posting_list[1].split(",")
    # Convert the gaps back to doc IDs
    list_doc_id = []
    previous_doc_id = 0
    for gap in decoded_doc_ids:
        doc_id = previous_doc_id + gap
        list_doc_id.append(doc_id)
        previous_doc_id = doc_id

    return list_doc_id, decoded_freqs

def decode_unary(bit_stream, limit=None):
    '''
    The decode_unary function processes the bit stream by counting consecutive 1s followed by a 0 to determine the
    original number (gap).

    :param bit_stream:
    :return: gaps
    '''
    gaps = []
    count = 0
    i = 0
    while i < len(bit_stream) and (limit is None or len(gaps) < limit):
        if bit_stream[i] == '1':
            count += 1
        elif bit_stream[i] == '0':
            gaps.append(count + 1)
            count = 0
        i += 1
    return gaps, bit_stream[i:]


def decode_gamma(bit_stream, limit=None):
    '''The decode_gamma function interprets the unary-coded length and the binary offset.
        It reconstructs the original number (gap) by combining the offset with the length.

        Padding isn’t an issue because:
            The decoder processes the bit stream in a specific pattern (e.g., identifying unary or gamma encodings).
            The padding will only be at the end, so it doesn’t interfere with valid encodings.
            In decode_gamma, once the bit stream is fully processed, any remaining bits (which would only be ones) are simply ignored.
    '''
    gaps = []
    i = 0
    while i < len(bit_stream) and (limit is None or len(gaps) < limit):
        length = 0
        while i < len(bit_stream) and bit_stream[i] == '1':
            length += 1
            i += 1
        if i < len(bit_stream) and bit_stream[i] == '0':
            i += 1

        if length > 0:
            offset = bit_stream[i:i + length - 1]
            i += length - 1
            if offset:
                gap = (1 << (length - 1)) + int(offset, 2)
            else:
                gap = 1 << (length - 1)
            gaps.append(gap)
    return gaps, bit_stream[i:]


