use crate::execution_manager::executor::AsyncExecutor;
use crate::execution_manager::packet::PacketTrait;
use crate::memory_data_size::MemoryDataSize;
use dashmap::DashMap;
use std::any::TypeId;
use std::cmp::max;
use std::io::stdout;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct MemoryTrackerManager {
    packet_sizes: DashMap<TypeId, ((usize, usize), usize)>,
    executors_sizes: DashMap<TypeId, ((usize, usize), usize)>,
    type_names: DashMap<TypeId, &'static str>,
}

unsafe impl Sync for MemoryTrackerManager {}
unsafe impl Send for MemoryTrackerManager {}

impl MemoryTrackerManager {
    pub fn new() -> Self {
        MemoryTrackerManager {
            packet_sizes: DashMap::new(),
            executors_sizes: DashMap::new(),
            type_names: DashMap::new(),
        }
    }

    pub fn get_executor_instance<E: AsyncExecutor>(self: &Arc<Self>) -> MemoryTracker<E> {
        MemoryTracker::new(self.clone())
    }

    pub fn add_queue_packet<T: PacketTrait>(&self, packet: &T) {
        if packet.get_size() > 0 {
            let mut entry = self
                .packet_sizes
                .entry(TypeId::of::<T>())
                .or_insert(((0, 0), 0));
            self.type_names
                .entry(TypeId::of::<T>())
                .or_insert(std::any::type_name::<T>());
            entry.value_mut().0 .0 += packet.get_size();
            entry.value_mut().0 .1 += 1;

            let crt_val = entry.value_mut().0 .0;
            let max_val = entry.value().1;
            entry.value_mut().1 = max(max_val, crt_val);
        }
    }
    pub fn remove_queue_packet<T: PacketTrait>(&self, packet: &T) {
        if packet.get_size() > 0 {
            let mut entry = self.packet_sizes.get_mut(&TypeId::of::<T>()).unwrap();
            entry.value_mut().0 .0 -= packet.get_size();
            entry.value_mut().0 .1 -= 1;
        }
    }

    fn get_pretty_name(&self, ptr: TypeId) -> String {
        let string = self.type_names.get(&ptr).unwrap();

        let mut builder = String::new();
        let mut last_was_col = false;
        for ch in string.chars() {
            builder.push(ch);

            let current_is_col = ch == ':';

            if last_was_col & current_is_col {
                builder.pop();
                builder.pop();

                while builder
                    .chars()
                    .last()
                    .map(|c| c.is_ascii_alphanumeric() || c == '_')
                    .unwrap_or(false)
                {
                    builder.pop();
                }

                last_was_col = false;
            } else {
                last_was_col = current_is_col;
            }
        }
        builder
    }

    pub fn print_debug(&self) {
        let _out = stdout().lock();

        crate::log_info!("Executors usages:");
        for executor in self.executors_sizes.iter() {
            crate::log_info!(
                "\t{} ==> {:.2} with {} instances [MAX {:.2}]",
                self.get_pretty_name(*executor.key()),
                MemoryDataSize::from_bytes(executor.value().0 .0),
                executor.value().0 .1,
                MemoryDataSize::from_bytes(executor.value().1)
            );
        }

        crate::log_info!("Packets in queue:");
        for packet in self.packet_sizes.iter() {
            crate::log_info!(
                "\t{} ==> {:.2} with {} instances [MAX {:.2}]",
                self.get_pretty_name(*packet.key()),
                MemoryDataSize::from_bytes(packet.value().0 .0),
                packet.value().0 .1,
                MemoryDataSize::from_bytes(packet.value().1)
            );
        }
    }
}

pub struct MemoryTracker<E: AsyncExecutor> {
    manager: Arc<MemoryTrackerManager>,
    last_memory_usage: usize,
    _phantom: PhantomData<E>,
}

impl<E: AsyncExecutor> Clone for MemoryTracker<E> {
    fn clone(&self) -> Self {
        self.manager
            .executors_sizes
            .get_mut(&TypeId::of::<E>())
            .unwrap()
            .value_mut()
            .0
             .1 += 1;
        Self {
            manager: self.manager.clone(),
            last_memory_usage: 0,
            _phantom: PhantomData,
        }
    }
}

impl<E: AsyncExecutor> MemoryTracker<E> {
    fn new(manager: Arc<MemoryTrackerManager>) -> Self {
        manager
            .executors_sizes
            .entry(TypeId::of::<E>())
            .or_insert(((0, 0), 0))
            .value_mut()
            .0
             .1 += 1;
        manager
            .type_names
            .entry(TypeId::of::<E>())
            .or_insert(std::any::type_name::<E>());

        MemoryTracker {
            manager,
            last_memory_usage: 0,
            _phantom: PhantomData,
        }
    }

    pub fn update_memory_usage(&mut self, usages: &[usize]) {
        let new_memory_usage = usages.iter().sum::<usize>();
        let mut entry = self
            .manager
            .executors_sizes
            .get_mut(&TypeId::of::<E>())
            .unwrap();
        entry.value_mut().0 .0 -= self.last_memory_usage;
        entry.value_mut().0 .0 += new_memory_usage;
        self.last_memory_usage = new_memory_usage;

        let crt_val = entry.value_mut().0 .0;
        let max_val = entry.value().1;
        entry.value_mut().1 = max(max_val, crt_val);
    }
}

impl<E: AsyncExecutor> Drop for MemoryTracker<E> {
    fn drop(&mut self) {
        let mut entry = self
            .manager
            .executors_sizes
            .get_mut(&TypeId::of::<E>())
            .unwrap();
        entry.value_mut().0 .0 -= self.last_memory_usage;
        entry.value_mut().0 .1 -= 1;
    }
}
