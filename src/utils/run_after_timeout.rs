use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

pub fn run_after_timeout<'a, F: 'a>(duration: Duration, f: F) -> Box<dyn FnOnce() -> bool>
where
    F: (FnOnce() -> ()) + Send + 'static,
{
    let (send, recv) = std::sync::mpsc::sync_channel::<bool>(0);
    let capture_send = send.clone();
    std::thread::spawn(move || {
        // Capturing send makes sure that cancellation Box need not to live
        let _send = capture_send;
        if let Err(RecvTimeoutError::Timeout) = recv.recv_timeout(duration) {
            f()
        }
    });

    Box::new(move || {
        if let Ok(()) = send.send(true) {
            return true;
        }
        return false;
    })
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };

    use super::run_after_timeout;

    #[test]
    fn it_cancels() {
        let canceller = run_after_timeout(Duration::from_millis(50), || {
            panic!("This should not run, because it's cancelled")
        });
        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(true, canceller());
    }

    #[test]
    fn it_runs() {
        let flipped = Arc::new(AtomicBool::new(false));
        let flipped_copy = flipped.clone();
        let canceller = run_after_timeout(Duration::from_millis(10), move || {
            flipped_copy.store(true, Ordering::Relaxed);
        });
        std::thread::sleep(Duration::from_millis(30));
        assert_eq!(false, canceller());
        assert_eq!(true, flipped.load(Ordering::Relaxed));
    }

    #[test]
    fn it_runs_without_cancel() {
        // This tests that `Sender` is captured, thus allowing the recv to live long enough
        let flipped = Arc::new(AtomicBool::new(false));
        let flipped_copy = flipped.clone();
        drop(run_after_timeout(Duration::from_millis(10), move || {
            flipped_copy.store(true, Ordering::Relaxed);
        }));
        std::thread::sleep(Duration::from_millis(30));
        assert_eq!(true, flipped.load(Ordering::Relaxed));
    }
}
