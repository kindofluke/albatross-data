use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use super::expr_converter::VeloxExpr;
use super::type_converter::VeloxType;

/// Velox plan node representation for FFI
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "node_type")]
pub enum VeloxPlanNode {
    #[serde(rename = "table_scan")]
    TableScan {
        table_name: String,
        file_path: String,
        columns: Vec<String>,
        column_types: Vec<String>,
    },
    #[serde(rename = "filter")]
    Filter {
        input: Box<VeloxPlanNode>,
        condition: VeloxExpr,
    },
    #[serde(rename = "project")]
    Project {
        input: Box<VeloxPlanNode>,
        projections: Vec<VeloxExpr>,
        names: Vec<String>,
    },
    #[serde(rename = "aggregate")]
    Aggregate {
        input: Box<VeloxPlanNode>,
        grouping_keys: Vec<VeloxExpr>,
        aggregates: Vec<AggregateFunction>,
    },
    #[serde(rename = "sort")]
    Sort {
        input: Box<VeloxPlanNode>,
        sort_keys: Vec<SortKey>,
    },
    #[serde(rename = "limit")]
    Limit {
        input: Box<VeloxPlanNode>,
        count: i64,
        offset: i64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateFunction {
    pub function: String,
    pub args: Vec<VeloxExpr>,
    pub output_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortKey {
    pub expr: VeloxExpr,
    pub ascending: bool,
    pub nulls_first: bool,
}

pub struct VeloxPlanBuilder {
    file_paths: Vec<PathBuf>,
    table_names: Vec<String>,
}

impl VeloxPlanBuilder {
    pub fn new(file_paths: Vec<PathBuf>, table_names: Vec<String>) -> Self {
        Self {
            file_paths,
            table_names,
        }
    }
    
    pub fn build_from_substrait(&self, plan: &substrait::Plan) -> Result<VeloxPlanNode> {
        // Extract the root relation
        let root_rel = self.extract_root_relation(plan)?;
        
        // Convert the relation tree to Velox plan nodes
        self.convert_relation(root_rel)
    }
    
    fn extract_root_relation<'a>(&self, plan: &'a substrait::Plan) -> Result<&'a substrait::Rel> {
        use substrait::plan_rel::RelType;
        
        let plan_rel = plan.relations.first()
            .ok_or_else(|| anyhow!("Plan has no relations"))?;
        
        let rel_type = plan_rel.rel_type.as_ref()
            .ok_or_else(|| anyhow!("Plan relation missing rel_type"))?;
        
        match rel_type {
            RelType::Root(root) => {
                root.input.as_ref()
                    .ok_or_else(|| anyhow!("Root relation has no input"))
            }
            RelType::Rel(rel) => Ok(rel),
        }
    }
    
    fn convert_relation(&self, rel: &substrait::Rel) -> Result<VeloxPlanNode> {
        use substrait::rel::RelType;
        
        let rel_type = rel.rel_type.as_ref()
            .ok_or_else(|| anyhow!("Relation missing rel_type"))?;
        
        match rel_type {
            RelType::Read(read) => self.convert_read(read),
            RelType::Filter(filter) => self.convert_filter(filter),
            RelType::Project(project) => self.convert_project(project),
            RelType::Aggregate(agg) => self.convert_aggregate(agg),
            RelType::Sort(sort) => self.convert_sort(sort),
            RelType::Fetch(fetch) => self.convert_fetch(fetch),
            _ => Err(anyhow!("Unsupported relation type: {:?}", rel_type)),
        }
    }
    
    fn convert_read(&self, read: &substrait::ReadRel) -> Result<VeloxPlanNode> {
        use substrait::read_rel::ReadType;
        
        let read_type = read.read_type.as_ref()
            .ok_or_else(|| anyhow!("Read relation missing read_type"))?;
        
        match read_type {
            ReadType::NamedTable(named_table) => {
                let table_name = named_table.names.first()
                    .ok_or_else(|| anyhow!("Named table has no names"))?;
                
                // Find the corresponding file path
                let file_path = self.table_names.iter()
                    .position(|name| name == table_name)
                    .and_then(|idx| self.file_paths.get(idx))
                    .ok_or_else(|| anyhow!("Table '{}' not found in file paths", table_name))?;
                
                // Extract schema information
                let base_schema = read.base_schema.as_ref()
                    .ok_or_else(|| anyhow!("Read relation missing base_schema"))?;
                
                let mut columns = Vec::new();
                let mut column_types = Vec::new();
                
                for (i, field_type) in base_schema.types.iter().enumerate() {
                    columns.push(format!("c{}", i));
                    let velox_type = VeloxType::from_substrait(field_type)?;
                    column_types.push(velox_type.to_type_string());
                }
                
                Ok(VeloxPlanNode::TableScan {
                    table_name: table_name.clone(),
                    file_path: file_path.to_string_lossy().to_string(),
                    columns,
                    column_types,
                })
            }
            _ => Err(anyhow!("Unsupported read type")),
        }
    }
    
    fn convert_filter(&self, filter: &substrait::FilterRel) -> Result<VeloxPlanNode> {
        let input = filter.input.as_ref()
            .ok_or_else(|| anyhow!("Filter relation missing input"))?;
        
        let condition = filter.condition.as_ref()
            .ok_or_else(|| anyhow!("Filter relation missing condition"))?;
        
        Ok(VeloxPlanNode::Filter {
            input: Box::new(self.convert_relation(input)?),
            condition: VeloxExpr::from_substrait(condition)?,
        })
    }
    
    fn convert_project(&self, project: &substrait::ProjectRel) -> Result<VeloxPlanNode> {
        let input = project.input.as_ref()
            .ok_or_else(|| anyhow!("Project relation missing input"))?;
        
        let mut projections = Vec::new();
        let mut names = Vec::new();
        
        for (i, expr) in project.expressions.iter().enumerate() {
            projections.push(VeloxExpr::from_substrait(expr)?);
            names.push(format!("col_{}", i));
        }
        
        Ok(VeloxPlanNode::Project {
            input: Box::new(self.convert_relation(input)?),
            projections,
            names,
        })
    }
    
    fn convert_aggregate(&self, agg: &substrait::AggregateRel) -> Result<VeloxPlanNode> {
        let input = agg.input.as_ref()
            .ok_or_else(|| anyhow!("Aggregate relation missing input"))?;
        
        let mut grouping_keys = Vec::new();
        for grouping in &agg.groupings {
            for expr in &grouping.grouping_expressions {
                grouping_keys.push(VeloxExpr::from_substrait(expr)?);
            }
        }
        
        let mut aggregates = Vec::new();
        for (i, measure) in agg.measures.iter().enumerate() {
            let agg_func = measure.measure.as_ref()
                .ok_or_else(|| anyhow!("Measure missing aggregate function"))?;
            
            let function_name = self.get_aggregate_function_name(agg_func.function_reference)?;
            
            let mut args = Vec::new();
            for arg in &agg_func.arguments {
                use substrait::function_argument::ArgType;
                
                let arg_type = arg.arg_type.as_ref()
                    .ok_or_else(|| anyhow!("Aggregate argument missing arg_type"))?;
                
                match arg_type {
                    ArgType::Value(expr) => {
                        args.push(VeloxExpr::from_substrait(expr)?);
                    }
                    _ => return Err(anyhow!("Unsupported aggregate argument type")),
                }
            }
            
            aggregates.push(AggregateFunction {
                function: function_name,
                args,
                output_name: format!("agg_{}", i),
            });
        }
        
        Ok(VeloxPlanNode::Aggregate {
            input: Box::new(self.convert_relation(input)?),
            grouping_keys,
            aggregates,
        })
    }
    
    fn convert_sort(&self, sort: &substrait::SortRel) -> Result<VeloxPlanNode> {
        let input = sort.input.as_ref()
            .ok_or_else(|| anyhow!("Sort relation missing input"))?;
        
        let mut sort_keys = Vec::new();
        for sort_field in &sort.sorts {
            let expr = sort_field.expr.as_ref()
                .ok_or_else(|| anyhow!("Sort field missing expression"))?;
            
            use substrait::sort_field::SortDirection;
            let ascending = match SortDirection::try_from(sort_field.direction) {
                Ok(SortDirection::AscNullsFirst) | Ok(SortDirection::AscNullsLast) => true,
                _ => false,
            };
            
            let nulls_first = match SortDirection::try_from(sort_field.direction) {
                Ok(SortDirection::AscNullsFirst) | Ok(SortDirection::DescNullsFirst) => true,
                _ => false,
            };
            
            sort_keys.push(SortKey {
                expr: VeloxExpr::from_substrait(expr)?,
                ascending,
                nulls_first,
            });
        }
        
        Ok(VeloxPlanNode::Sort {
            input: Box::new(self.convert_relation(input)?),
            sort_keys,
        })
    }
    
    fn convert_fetch(&self, fetch: &substrait::FetchRel) -> Result<VeloxPlanNode> {
        let input = fetch.input.as_ref()
            .ok_or_else(|| anyhow!("Fetch relation missing input"))?;
        
        Ok(VeloxPlanNode::Limit {
            input: Box::new(self.convert_relation(input)?),
            count: fetch.count,
            offset: fetch.offset,
        })
    }
    
    fn get_aggregate_function_name(&self, func_ref: u32) -> Result<String> {
        // Simplified mapping - in practice, look up in extension section
        let name = match func_ref {
            0 => "count",
            1 => "sum",
            2 => "avg",
            3 => "min",
            4 => "max",
            _ => return Err(anyhow!("Unknown aggregate function reference: {}", func_ref)),
        };
        Ok(name.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_scan_serialization() {
        let node = VeloxPlanNode::TableScan {
            table_name: "orders".to_string(),
            file_path: "/path/to/orders.parquet".to_string(),
            columns: vec!["c0".to_string(), "c1".to_string()],
            column_types: vec!["INTEGER".to_string(), "VARCHAR".to_string()],
        };
        
        let json = serde_json::to_string(&node).unwrap();
        assert!(json.contains("\"node_type\":\"table_scan\""));
        assert!(json.contains("\"table_name\":\"orders\""));
    }
}
