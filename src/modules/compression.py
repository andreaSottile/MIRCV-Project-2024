from src.modules.utils import print_log


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


def decode_posting_list(compressed_bytes, compression="no"):
    '''
    The decode_posting_list function first reads the binary data and converts it to a string of bits.
    :param compression:
    :param compressed_bytes:
    :return: list_doc_id
    '''
    if compression != "no":
        # Convert the bytes back into a bit string
        bit_stream = ''.join(f'{byte:08b}' for byte in compressed_bytes)
        print(bit_stream)

        # Step 2: Decode the doc IDs
        if compression == "unary":
            decoded_numbers = decode_unary(bit_stream)
        elif compression == "gamma":
            decoded_numbers = decode_gamma(bit_stream)
        else:
            print_log("Critical error: compression method not found")
            return ""
        half_doc_number = int(len(decoded_numbers) / 2)
        decoded_doc_ids = decoded_numbers[:half_doc_number]
        decoded_freqs = decoded_numbers[half_doc_number:]
    else:
        posting_list = compressed_bytes.split()
        decoded_doc_ids = map(int, posting_list[0].split(","))
        decoded_freqs = posting_list[1].split(",")
    # Convert the gaps back to doc IDs
    # list_doc_id = []
    list_doc_id_string = ""
    previous_doc_id = 0
    first = True
    for gap in decoded_doc_ids:
        doc_id = previous_doc_id + gap
        if first:
            list_doc_id_string += str(doc_id)
            first = False
        else:
            list_doc_id_string += "," + str(doc_id)
        # list_doc_id.append(doc_id)
        previous_doc_id = doc_id
    posting_list_string_decoded = list_doc_id_string + " "
    first = True
    for freq in decoded_freqs:
        if first:
            posting_list_string_decoded += str(freq)
            first = False
        else:
            posting_list_string_decoded += "," + str(freq)
    return posting_list_string_decoded

    # return list_doc_id, decoded_freqs


def decode_unary(bit_stream):
    '''
    The decode_unary function processes the bit stream by counting consecutive 1s followed by a 0 to determine the
    original number (gap).

    :param bit_stream:
    :return: gaps
    '''
    gaps = []
    count = 0
    i = 0
    while i < len(bit_stream):
        if bit_stream[i] == '1':
            count += 1
        elif bit_stream[i] == '0':
            gaps.append(count + 1)
            count = 0
        i += 1
    return gaps


def decode_gamma(bit_stream):
    '''The decode_gamma function interprets the unary-coded length and the binary offset.
        It reconstructs the original number (gap) by combining the offset with the length.

        number 4 will be 110.00
        First part: Unary encoding of the length of the binary representation
                    binary of number 4 is 100 so its length is 3 bit, the Unary of 3 is 110
        Second part: Offset, i.e., the binary representation without the most significant bit
                     binary of number 4 is 100 so its representation without the most significant bit is 00

        Padding isn’t an issue because:
            The decoder processes the bit stream in a specific pattern (e.g., identifying unary or gamma encodings).
            The padding will only be at the end, so it doesn’t interfere with valid encodings.
            In decode_gamma, once the bit stream is fully processed, any remaining bits (which would only be ones) are simply ignored.
    '''
    gaps = []
    i = 0
    while i < len(bit_stream):
        length = 0
        padding = True
        while i < len(bit_stream) and bit_stream[i] == '1':
            length += 1
            i += 1
        if i < len(bit_stream) and bit_stream[i] == '0':
            i += 1
            padding = False
        if not padding:
            if length > 0:
                offset = bit_stream[i:i + length]
                i += length
                if offset:
                    gap = (1 << length) + int(offset, 2)
                else:
                    gap = 1 << length
                gaps.append(gap)
            else:
                gap = 1 << length
                gaps.append(gap)
    return gaps
