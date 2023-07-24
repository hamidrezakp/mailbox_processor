/// Can be used to raise notifications out of a mailbox. Notifications
/// aren't guaranteed to arrive, and the mailbox has no way to know
/// when (of if) they were received.
///
/// Use of this type is completely optional, but it implements the
/// best practices for raising notifications from mailboxes and its
/// use in this scenario is highly recommended.
use tokio::sync::mpsc;

/// The sender part of NotificationChannel.
#[derive(Clone)]
pub struct Sender<T> {
    sender: mpsc::UnboundedSender<T>,
}

impl<T> Sender<T> {
    /// Send a notification into channel.
    pub fn send(&self, notification: T) {
        // Notifications aren't guaranteed to arrive, and we don't need to handle
        // closed receivers.
        let _ = self.sender.send(notification);
    }
}

/// The receiver part of NotificationChannel.
pub type Receiver<T> = mpsc::UnboundedReceiver<T>;

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    (Sender { sender }, receiver)
}
