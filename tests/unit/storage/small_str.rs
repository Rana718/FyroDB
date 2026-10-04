    #[test]
    fn binary_values_round_trip_across_inline_and_heap_storage() {
        for size in [0, 1, 23, 24, 4096] {
            let bytes: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
            let value = super::SmallStr::from_slice(&bytes);
            assert_eq!(value.as_bytes(), bytes);
            assert_eq!(value.clone().into_bytes(), bytes);
            assert_eq!(super::SmallStr::from_bytes(bytes.clone()).into_bytes(), bytes);
        }
    }

    use super::SmallStr;

    #[test]
    fn clone_is_an_independent_copy() {
        let long = "0123456789abcdefghijklmnopqrstuvwxyz";
        let a = SmallStr::new(long);
        let b = a.clone();
        assert_eq!(a.as_str(), long);
        assert_eq!(b.as_str(), long);
        // Heap variant must not alias: two owners of one box would double-free.
        if long.len() > super::SMALL_STR_CAP {
            assert_ne!(
                a.as_str().as_ptr(),
                b.as_str().as_ptr(),
                "cloned heap variant aliases the original allocation"
            );
        }
        let mut c = a.clone();
        c.push_str("x");
        assert_eq!(a.as_str(), long);
        assert_eq!(c.as_str(), "0123456789abcdefghijklmnopqrstuvwxyzx");
    }

    #[test]
    fn layout_is_24_bytes_with_23_inline() {
        assert_eq!(std::mem::size_of::<SmallStr>(), 24);
        let s = SmallStr::new("01234567890123456789012");
        assert_eq!(s.len(), 23);
        assert_ne!(s.len, super::Tag::Heap);
        let h = SmallStr::new("012345678901234567890123");
        assert_eq!(h.len, super::Tag::Heap);
        assert_eq!(h.as_str().len(), 24);
    }

    #[test]
    fn push_str_stays_inline_when_it_fits() {
        let mut s = SmallStr::new("hello");
        s.push_str(" world"); // 11 bytes total, fits inline
        assert_eq!(s.as_str(), "hello world");
    }
