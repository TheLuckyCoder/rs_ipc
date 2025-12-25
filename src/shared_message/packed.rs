#[derive(Default, Clone, Copy)]
pub(crate) struct SequenceState {
    pub sequence: u64,
    pub stopped: bool,
    pub writing_in_progress: bool,
}

impl SequenceState {
    pub const STOPPED_MASK: u64 = 1u64 << 63;
    pub const WRITING_IN_PROGRESS_MASK: u64 = 1u64 << 62;
    pub const SEQUENCE_MASK: u64 = !(Self::STOPPED_MASK | Self::WRITING_IN_PROGRESS_MASK);

    #[inline]
    pub(crate) fn from(packed: u64) -> Self {
        Self {
            sequence: packed & Self::SEQUENCE_MASK,
            stopped: (packed & Self::STOPPED_MASK) != 0,
            writing_in_progress: (packed & Self::WRITING_IN_PROGRESS_MASK) != 0,
        }
    }

    #[inline]
    pub(crate) fn to_packed(&self) -> u64 {
        ((self.stopped as u64) << 63)
            | ((self.writing_in_progress as u64) << 62)
            | self.sequence & Self::SEQUENCE_MASK
    }
}

#[derive(Default, Clone, Copy)]
pub(crate) struct ReadersStateCount {
    pub target_read: u16,
    pub consumers: u16,
    pub active_readers: u16,
    pub data_consumed: u16,
}

impl ReadersStateCount {
    const TARGET_SHIFT: u32 = u16::BITS * 0;
    const CONSUMERS_SHIFT: u32 = u16::BITS * 1;
    const ACTIVE_SHIFT: u32 = u16::BITS * 2;
    const CONSUMED_SHIFT: u32 = u16::BITS * 3;

    #[inline]
    pub(crate) fn from(packed: u64) -> Self {
        Self {
            target_read: (packed >> Self::TARGET_SHIFT) as u16,
            consumers: (packed >> Self::CONSUMERS_SHIFT) as u16,
            active_readers: (packed >> Self::ACTIVE_SHIFT) as u16,
            data_consumed: (packed >> Self::CONSUMED_SHIFT) as u16,
        }
    }

    #[inline]
    pub(crate) fn to_packed(&self) -> u64 {
        (self.target_read as u64) << Self::TARGET_SHIFT
            | (self.consumers as u64) << Self::CONSUMERS_SHIFT
            | (self.active_readers as u64) << Self::ACTIVE_SHIFT
            | (self.data_consumed as u64) << Self::CONSUMED_SHIFT
    }

    #[inline]
    pub(crate) fn get_target_consumed(&self) -> u16 {
        self.target_read.min(self.consumers)
    }
}
