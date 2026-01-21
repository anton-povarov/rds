use memchr::memchr;

pub fn find_crlf(buf: &[u8]) -> Option<usize> {
    let mut pos = 0;
    while let Some(i) = memchr(b'\r', &buf[pos..]) {
        let idx = pos + i;
        if idx + 1 < buf.len() && buf[idx + 1] == b'\n' {
            return Some(idx);
        }
        pos = idx + 1;
    }
    None
}
