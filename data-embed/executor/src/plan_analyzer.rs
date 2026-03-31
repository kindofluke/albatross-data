use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Analyzes a physical plan to determine if it's suitable for GPU execution
pub struct GpuSuitabilityAnalysis {
    pub can_use_gpu: bool,
    pub reason: String,
    pub operation_type: OperationType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OperationType {
    Aggregation,
    HashJoin,
    Filter,
    TableScan,
    Window,
    Complex,
    Unsupported,
}

impl GpuSuitabilityAnalysis {
    /// Analyze a physical plan to determine GPU suitability
    pub fn analyze(plan: Arc<dyn ExecutionPlan>) -> Self {
        let plan_name = plan.name();

        // Inspect the physical plan node type
        match plan_name {
            // Aggregations are good GPU candidates
            "AggregateExec" => {
                // Check if it's a simple aggregation (SUM, COUNT, etc.)
                let children = plan.children();
                if children.len() == 1 {
                    Self {
                        can_use_gpu: true,
                        reason: "Simple aggregation detected - GPU accelerated".to_string(),
                        operation_type: OperationType::Aggregation,
                    }
                } else {
                    Self {
                        can_use_gpu: false,
                        reason: "Complex aggregation - using CPU".to_string(),
                        operation_type: OperationType::Complex,
                    }
                }
            }

            // Hash joins can benefit from GPU
            "HashJoinExec" => {
                Self {
                    can_use_gpu: true,
                    reason: "Hash join detected - GPU accelerated".to_string(),
                    operation_type: OperationType::HashJoin,
                }
            }

            // Window functions (OVER clause)
            "WindowAggExec" | "BoundedWindowAggExec" => {
                Self {
                    can_use_gpu: true,
                    reason: "Window function detected - GPU accelerated".to_string(),
                    operation_type: OperationType::Window,
                }
            }

            // Filters can be GPU accelerated
            "FilterExec" => {
                // Recurse into child to see what we're filtering
                let children = plan.children();
                if !children.is_empty() {
                    let child_analysis = Self::analyze(children[0].clone());
                    if child_analysis.can_use_gpu {
                        return child_analysis; // Use GPU for the child operation
                    }
                }

                Self {
                    can_use_gpu: false,
                    reason: "Filter over non-GPU operation".to_string(),
                    operation_type: OperationType::Filter,
                }
            }

            // Projection (SELECT columns)
            "ProjectionExec" => {
                // Check what we're projecting from
                let children = plan.children();
                if !children.is_empty() {
                    return Self::analyze(children[0].clone());
                }

                Self {
                    can_use_gpu: false,
                    reason: "Projection without source".to_string(),
                    operation_type: OperationType::TableScan,
                }
            }

            // CoalescePartitionsExec - just passes through, check child
            "CoalescePartitionsExec" | "RepartitionExec" => {
                let children = plan.children();
                if !children.is_empty() {
                    return Self::analyze(children[0].clone());
                }

                Self {
                    can_use_gpu: false,
                    reason: "Repartition operation".to_string(),
                    operation_type: OperationType::Unsupported,
                }
            }

            // Table scans - these should stay on CPU for now
            "ParquetExec" | "CsvExec" | "MemoryExec" => {
                Self {
                    can_use_gpu: false,
                    reason: "Simple table scan - CPU is sufficient".to_string(),
                    operation_type: OperationType::TableScan,
                }
            }

            // Default: check children recursively
            _ => {
                let children = plan.children();
                if !children.is_empty() {
                    // If any child can use GPU, propagate that up
                    for child in children {
                        let child_analysis = Self::analyze(child.clone());
                        if child_analysis.can_use_gpu {
                            return child_analysis;
                        }
                    }
                }

                Self {
                    can_use_gpu: false,
                    reason: format!("Unsupported operation: {}", plan_name),
                    operation_type: OperationType::Unsupported,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_type_equality() {
        assert_eq!(OperationType::Aggregation, OperationType::Aggregation);
        assert_ne!(OperationType::Aggregation, OperationType::HashJoin);
    }
}
