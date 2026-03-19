use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

/// Velox expression representation for FFI
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum VeloxExpr {
    #[serde(rename = "field")]
    FieldReference {
        field: String,
    },
    #[serde(rename = "constant")]
    Constant {
        value: serde_json::Value,
        data_type: String,
    },
    #[serde(rename = "call")]
    FunctionCall {
        function: String,
        args: Vec<VeloxExpr>,
    },
}

impl VeloxExpr {
    /// Convert from substrait expression
    pub fn from_substrait(expr: &substrait::Expression) -> Result<Self> {
        use substrait::expression::RexType;
        
        let rex_type = expr.rex_type.as_ref()
            .ok_or_else(|| anyhow!("Expression missing rex_type"))?;
        
        match rex_type {
            RexType::Selection(selection) => {
                Self::from_field_reference(selection)
            }
            RexType::Literal(literal) => {
                Self::from_literal(literal)
            }
            RexType::ScalarFunction(func) => {
                Self::from_scalar_function(func)
            }
            _ => Err(anyhow!("Unsupported expression type: {:?}", rex_type)),
        }
    }
    
    fn from_field_reference(selection: &substrait::expression::FieldReference) -> Result<Self> {
        use substrait::expression::field_reference::ReferenceType;
        
        let ref_type = selection.reference_type.as_ref()
            .ok_or_else(|| anyhow!("Field reference missing reference_type"))?;
        
        match ref_type {
            ReferenceType::DirectReference(direct) => {
                use substrait::expression::reference_segment::ReferenceType as SegmentType;
                
                let segment = direct.reference_type.as_ref()
                    .ok_or_else(|| anyhow!("Direct reference missing segment"))?;
                
                match segment {
                    SegmentType::StructField(field) => {
                        Ok(VeloxExpr::FieldReference {
                            field: format!("c{}", field.field),
                        })
                    }
                    _ => Err(anyhow!("Unsupported reference segment type")),
                }
            }
            _ => Err(anyhow!("Unsupported field reference type")),
        }
    }
    
    fn from_literal(literal: &substrait::expression::Literal) -> Result<Self> {
        use substrait::expression::literal::LiteralType;
        
        let lit_type = literal.literal_type.as_ref()
            .ok_or_else(|| anyhow!("Literal missing literal_type"))?;
        
        match lit_type {
            LiteralType::Boolean(b) => Ok(VeloxExpr::Constant {
                value: serde_json::Value::Bool(*b),
                data_type: "BOOLEAN".to_string(),
            }),
            LiteralType::I8(i) => Ok(VeloxExpr::Constant {
                value: serde_json::Value::Number((*i).into()),
                data_type: "TINYINT".to_string(),
            }),
            LiteralType::I16(i) => Ok(VeloxExpr::Constant {
                value: serde_json::Value::Number((*i).into()),
                data_type: "SMALLINT".to_string(),
            }),
            LiteralType::I32(i) => Ok(VeloxExpr::Constant {
                value: serde_json::Value::Number((*i).into()),
                data_type: "INTEGER".to_string(),
            }),
            LiteralType::I64(i) => Ok(VeloxExpr::Constant {
                value: serde_json::Value::Number((*i).into()),
                data_type: "BIGINT".to_string(),
            }),
            LiteralType::Fp32(f) => Ok(VeloxExpr::Constant {
                value: serde_json::Number::from_f64(*f as f64)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null),
                data_type: "REAL".to_string(),
            }),
            LiteralType::Fp64(f) => Ok(VeloxExpr::Constant {
                value: serde_json::Number::from_f64(*f)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null),
                data_type: "DOUBLE".to_string(),
            }),
            LiteralType::String(s) => Ok(VeloxExpr::Constant {
                value: serde_json::Value::String(s.clone()),
                data_type: "VARCHAR".to_string(),
            }),
            _ => Err(anyhow!("Unsupported literal type: {:?}", lit_type)),
        }
    }
    
    fn from_scalar_function(func: &substrait::expression::ScalarFunction) -> Result<Self> {
        let function_name = Self::get_function_name(func.function_reference)?;
        
        let mut args = Vec::new();
        for arg in &func.arguments {
            use substrait::function_argument::ArgType;
            
            let arg_type = arg.arg_type.as_ref()
                .ok_or_else(|| anyhow!("Function argument missing arg_type"))?;
            
            match arg_type {
                ArgType::Value(expr) => {
                    args.push(Self::from_substrait(expr)?);
                }
                _ => return Err(anyhow!("Unsupported function argument type")),
            }
        }
        
        Ok(VeloxExpr::FunctionCall {
            function: function_name,
            args,
        })
    }
    
    fn get_function_name(func_ref: u32) -> Result<String> {
        // This is a simplified mapping - in practice, you'd need to look up
        // the function reference in the substrait plan's extension section
        // For now, we'll use common function IDs
        let name = match func_ref {
            0 => "add",
            1 => "subtract",
            2 => "multiply",
            3 => "divide",
            4 => "eq",
            5 => "neq",
            6 => "lt",
            7 => "lte",
            8 => "gt",
            9 => "gte",
            10 => "and",
            11 => "or",
            12 => "not",
            _ => return Err(anyhow!("Unknown function reference: {}", func_ref)),
        };
        Ok(name.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_reference_serialization() {
        let expr = VeloxExpr::FieldReference {
            field: "c0".to_string(),
        };
        let json = serde_json::to_string(&expr).unwrap();
        assert!(json.contains("\"type\":\"field\""));
        assert!(json.contains("\"field\":\"c0\""));
    }

    #[test]
    fn test_constant_serialization() {
        let expr = VeloxExpr::Constant {
            value: serde_json::Value::Number(42.into()),
            data_type: "INTEGER".to_string(),
        };
        let json = serde_json::to_string(&expr).unwrap();
        assert!(json.contains("\"type\":\"constant\""));
        assert!(json.contains("\"value\":42"));
    }

    #[test]
    fn test_function_call_serialization() {
        let expr = VeloxExpr::FunctionCall {
            function: "add".to_string(),
            args: vec![
                VeloxExpr::FieldReference { field: "c0".to_string() },
                VeloxExpr::Constant {
                    value: serde_json::Value::Number(1.into()),
                    data_type: "INTEGER".to_string(),
                },
            ],
        };
        let json = serde_json::to_string(&expr).unwrap();
        assert!(json.contains("\"type\":\"call\""));
        assert!(json.contains("\"function\":\"add\""));
    }
}
