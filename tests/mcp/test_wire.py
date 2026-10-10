from pathlib import Path
import io
import os
import sys
import threading
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'packages/client/src'))
sys.path.insert(0, str(ROOT / 'packages/mcp/src'))
from equity_feature_mcp import _wire


class Wire(unittest.TestCase):
    def test_actual_pipe_input_overflow_and_exact_limit(self):
        for size in (16384, 16385):
            reader, writer = os.pipe()
            data = b' ' * (size-3) + b'{}\n'
            def send():
                with os.fdopen(writer, 'wb', buffering=0) as out: out.write(data)
            thread = threading.Thread(target=send)
            thread.start()
            with os.fdopen(reader, 'rb', buffering=0) as stream:
                if size == 16384:
                    frame = _wire.read_frame(stream)
                    self.assertEqual(_wire.parse(frame), {})
                else:
                    with self.assertRaisesRegex(_wire.FrameError, 'input_bounds'): _wire.read_frame(stream)
            thread.join(2)
            self.assertFalse(thread.is_alive())

    def test_duplicate_nonfinite_utf8_and_depth_denials(self):
        for data in (b'{"x":1,"x":2}\n', b'{"x":NaN}\n', b'{"x":1e100000}\n', b'{"x":"\xff"}\n', b'['*33+b'0'+b']'*33+b'\n'):
            with self.subTest(data=data[:20]), self.assertRaisesRegex(_wire.FrameError, 'invalid_json'): _wire.parse(data)
        self.assertEqual(_wire.parse(b'{"quote":"[\\\"{","x":-0.0}\n')['quote'], '["{')

    def test_nodes_eof_and_frame_separation(self):
        # Defensive node limit is separate from the stricter physical input
        # cap: this large Python value is not an admitted wire frame.
        _wire._nodes([0] * 9999)
        with self.assertRaises(_wire.FrameError): _wire._nodes([0] * 10000)
        with self.assertRaises(_wire.FrameError): _wire.parse(b'['+b'0,'*10000+b'0]\n')
        stream = io.BytesIO(b'{}\n{}\n')
        self.assertEqual(_wire.read_frame(stream), b'{}\n')
        self.assertEqual(_wire.read_frame(stream), b'{}\n')
        self.assertIsNone(_wire.read_frame(stream))
        with self.assertRaisesRegex(_wire.FrameError, 'partial_eof'): _wire.read_frame(io.BytesIO(b'{}'))
        closed = io.BytesIO()
        closed.close()
        with self.assertRaisesRegex(_wire.FrameError, 'input_failed'): _wire.read_frame(closed)

    def test_encoded_output_boundary_and_no_replacement_after_short_write(self):
        self.assertEqual(len(_wire.encode_frame('x'*65533)), 65536)
        with self.assertRaises(_wire.FrameError): _wire.encode_frame('x'*65534)
        class Short(io.BytesIO):
            def write(self, data): return super().write(data[:3])
        stream = Short()
        with self.assertRaisesRegex(_wire.FrameError, 'incomplete_output'): _wire.write_frame(stream, b'{"ok":true}\n')
        self.assertEqual(stream.getvalue(), b'{"o')

    def test_actual_closed_pipe_output_fails_without_retry(self):
        reader, writer = os.pipe()
        os.close(reader)
        with os.fdopen(writer, 'wb', buffering=0) as stream:
            with self.assertRaisesRegex(_wire.FrameError, 'incomplete_output'): _wire.write_frame(stream, b'{}\n')


if __name__ == '__main__': unittest.main()
