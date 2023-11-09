# macro-utils

A utility crate of macros for generating test data for `DatenLord`.
The main purpose of this crate is to generate FUSE request packets.

## Examples
```rust
use macro_utils::{generate_bytes, calculate_size};

const DATA: [u8; 40] = generate_bytes! {
    BE;         // Endian
    u64: 65,    // field 1
    u64: 20,    // ...
    u64: 507,
    u32: 0x20,
    u32: 0x20,
    str: b"foo.bar\0",  // bytes
};
const EXPECTED: [u8; 40] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x41,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xfb,
    0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x20,
    0x66, 0x6f, 0x6f, 0x2e, 0x62, 0x61, 0x72, 0x00
];
assert_eq!(DATA, EXPECTED);

const SIZE: usize = calculate_size! {
    BE;         // This field makes no difference but cannot be ignored.
    u64: 65,
    u64: 20,
    u64: 507,
    u32: 0x20,
    u32: 0x20,
    str: b"foo.bar\0",
};
assert_eq!(SIZE, 40);
```