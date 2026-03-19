use anyhow::{anyhow, Context, Result};
use prost::Message;

pub fn parse_substrait_plan(bytes: &[u8]) -> Result<substrait::Plan> {
    substrait::Plan::decode(bytes)
        .context("Failed to decode substrait plan")
}

pub fn extract_relations(plan: &substrait::Plan) -> Result<Vec<&substrait::Rel>> {
    let mut relations = Vec::new();
    
    for relation in &plan.relations {
        use substrait::plan_rel::RelType;
        
        let rel_type = relation.rel_type.as_ref()
            .ok_or_else(|| anyhow!("Plan relation missing rel_type"))?;
        
        match rel_type {
            RelType::Root(root) => {
                if let Some(input) = &root.input {
                    relations.push(input);
                }
            }
            RelType::Rel(rel) => {
                relations.push(rel);
            }
        }
    }
    
    Ok(relations)
}

pub fn get_relation_type(rel: &substrait::Rel) -> Result<&str> {
    use substrait::rel::RelType;
    
    let rel_type = rel.rel_type.as_ref()
        .ok_or_else(|| anyhow!("Relation missing rel_type"))?;
    
    let type_name = match rel_type {
        RelType::Read(_) => "Read",
        RelType::Filter(_) => "Filter",
        RelType::Project(_) => "Project",
        RelType::Aggregate(_) => "Aggregate",
        RelType::Sort(_) => "Sort",
        RelType::Join(_) => "Join",
        RelType::Fetch(_) => "Fetch",
        RelType::Cross(_) => "Cross",
        RelType::Set(_) => "Set",
        _ => "Unknown",
    };
    
    Ok(type_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty_plan() {
        let plan = substrait::Plan::default();
        let mut buf = Vec::new();
        plan.encode(&mut buf).unwrap();
        
        let parsed = parse_substrait_plan(&buf).unwrap();
        assert_eq!(parsed.relations.len(), 0);
    }
}
