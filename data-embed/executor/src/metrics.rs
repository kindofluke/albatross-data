use anyhow::Result;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone, Default)]
pub struct GpuMetrics {
    pub peak_utilization: u32,
    pub peak_memory_mb: u32,
    pub avg_utilization: f64,
    pub avg_memory_mb: f64,
    pub samples: usize,
}

impl GpuMetrics {
    pub fn new() -> Self {
        Self::default()
    }
}

pub struct GpuMonitor {
    metrics: Arc<Mutex<GpuMetrics>>,
    running: Arc<Mutex<bool>>,
}

impl GpuMonitor {
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(Mutex::new(GpuMetrics::new())),
            running: Arc::new(Mutex::new(false)),
        }
    }

    pub fn start(&self) -> Result<()> {
        *self.running.lock().unwrap() = true;
        
        let metrics = self.metrics.clone();
        let running = self.running.clone();

        thread::spawn(move || {
            let mut total_util = 0u64;
            let mut total_mem = 0u64;
            let mut samples = 0usize;

            while *running.lock().unwrap() {
                // Query nvidia-smi for GPU stats
                if let Ok(output) = Command::new("nvidia-smi")
                    .args(&[
                        "--query-gpu=utilization.gpu,memory.used",
                        "--format=csv,noheader,nounits",
                    ])
                    .stdout(Stdio::piped())
                    .stderr(Stdio::null())
                    .output()
                {
                    if let Ok(stdout) = String::from_utf8(output.stdout) {
                        if let Some(line) = stdout.lines().next() {
                            let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
                            if parts.len() == 2 {
                                if let (Ok(util), Ok(mem)) = (
                                    parts[0].parse::<u32>(),
                                    parts[1].parse::<u32>(),
                                ) {
                                    let mut m = metrics.lock().unwrap();
                                    m.peak_utilization = m.peak_utilization.max(util);
                                    m.peak_memory_mb = m.peak_memory_mb.max(mem);
                                    total_util += util as u64;
                                    total_mem += mem as u64;
                                    samples += 1;
                                    m.samples = samples;
                                    m.avg_utilization = total_util as f64 / samples as f64;
                                    m.avg_memory_mb = total_mem as f64 / samples as f64;
                                }
                            }
                        }
                    }
                }

                thread::sleep(Duration::from_millis(100));
            }
        });

        Ok(())
    }

    pub fn stop(&self) -> GpuMetrics {
        *self.running.lock().unwrap() = false;
        thread::sleep(Duration::from_millis(150)); // Wait for last sample
        self.metrics.lock().unwrap().clone()
    }
}
