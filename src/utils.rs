// transforms &[u8] into a String with non-printable/non-ascii characters escaped
// https://stackoverflow.com/questions/41449708/how-to-print-a-u8-slice-as-text-if-i-dont-care-about-the-particular-encoding
pub fn u8_escaped(buf: &[u8]) -> String {
    let v = buf.iter().map(|b| std::ascii::escape_default(*b)).flatten().collect();
    return String::from_utf8(v).unwrap();
}
