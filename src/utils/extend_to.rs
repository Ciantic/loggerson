pub trait ExtendTo<T, I, R>
where
    R: Extend<T>,
    I: IntoIterator<Item = T>,
{
    /// Moves the values to mutable reference which implements `extend()`
    fn extend_to(self, mutref: &mut R) -> ();
}

impl<T, I, R> ExtendTo<T, I, R> for I
where
    R: Extend<T>,
    I: IntoIterator<Item = T>,
{
    /// Moves the values to mutable reference which implements `extend()`
    fn extend_to(self, mutref: &mut R) {
        mutref.extend(self);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::ExtendTo;

    #[test]
    fn extend_to_vec() {
        let input = vec![4, 5];
        let mut receiver = vec![1, 2, 3];
        input.iter().extend_to(&mut receiver);
        assert_eq!(vec![1, 2, 3, 4, 5], receiver);
    }

    #[test]
    fn extend_to_hashmap() {
        let input = vec![("a", 1), ("b", 2)];
        let mut receiver: HashMap<_, _> = vec![("c", 3), ("d", 4)].into_iter().collect();

        // Notice: into_iter is required
        input.into_iter().extend_to(&mut receiver);
        let mut gotem = receiver.into_iter().collect::<Vec<_>>();
        gotem.sort();
        assert_eq!(vec![("a", 1), ("b", 2), ("c", 3), ("d", 4)], gotem);
    }
}
