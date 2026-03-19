use anyhow::{anyhow, Result};

/// Velox type representation for FFI
#[derive(Debug, Clone)]
pub enum VeloxType {
    Boolean,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Real,
    Double,
    Varchar,
    Varbinary,
    Timestamp,
    Date,
    Array(Box<VeloxType>),
    Map(Box<VeloxType>, Box<VeloxType>),
    Row(Vec<(String, VeloxType)>),
}

impl VeloxType {
    /// Convert from substrait type
    pub fn from_substrait(substrait_type: &substrait::Type) -> Result<Self> {
        use substrait::r#type::Kind;
        
        let kind = substrait_type.kind.as_ref()
            .ok_or_else(|| anyhow!("Substrait type missing kind"))?;
        
        match kind {
            Kind::Bool(_) => Ok(VeloxType::Boolean),
            Kind::I8(_) => Ok(VeloxType::TinyInt),
            Kind::I16(_) => Ok(VeloxType::SmallInt),
            Kind::I32(_) => Ok(VeloxType::Integer),
            Kind::I64(_) => Ok(VeloxType::BigInt),
            Kind::Fp32(_) => Ok(VeloxType::Real),
            Kind::Fp64(_) => Ok(VeloxType::Double),
            Kind::String(_) => Ok(VeloxType::Varchar),
            Kind::Binary(_) => Ok(VeloxType::Varbinary),
            Kind::Timestamp(_) => Ok(VeloxType::Timestamp),
            Kind::Date(_) => Ok(VeloxType::Date),
            Kind::List(list) => {
                let element_type = list.r#type.as_ref()
                    .ok_or_else(|| anyhow!("List type missing element type"))?;
                Ok(VeloxType::Array(Box::new(Self::from_substrait(element_type)?)))
            }
            Kind::Map(map) => {
                let key_type = map.key.as_ref()
                    .ok_or_else(|| anyhow!("Map type missing key type"))?;
                let value_type = map.value.as_ref()
                    .ok_or_else(|| anyhow!("Map type missing value type"))?;
                Ok(VeloxType::Map(
                    Box::new(Self::from_substrait(key_type)?),
                    Box::new(Self::from_substrait(value_type)?),
                ))
            }
            Kind::Struct(struct_type) => {
                let mut fields = Vec::new();
                for (i, field_type) in struct_type.types.iter().enumerate() {
                    let field_name = format!("field_{}", i);
                    fields.push((field_name, Self::from_substrait(field_type)?));
                }
                Ok(VeloxType::Row(fields))
            }
            _ => Err(anyhow!("Unsupported substrait type: {:?}", kind)),
        }
    }
    
    /// Convert to type string for FFI
    pub fn to_type_string(&self) -> String {
        match self {
            VeloxType::Boolean => "BOOLEAN".to_string(),
            VeloxType::TinyInt => "TINYINT".to_string(),
            VeloxType::SmallInt => "SMALLINT".to_string(),
            VeloxType::Integer => "INTEGER".to_string(),
            VeloxType::BigInt => "BIGINT".to_string(),
            VeloxType::Real => "REAL".to_string(),
            VeloxType::Double => "DOUBLE".to_string(),
            VeloxType::Varchar => "VARCHAR".to_string(),
            VeloxType::Varbinary => "VARBINARY".to_string(),
            VeloxType::Timestamp => "TIMESTAMP".to_string(),
            VeloxType::Date => "DATE".to_string(),
            VeloxType::Array(elem) => format!("ARRAY<{}>", elem.to_type_string()),
            VeloxType::Map(key, val) => {
                format!("MAP<{},{}>", key.to_type_string(), val.to_type_string())
            }
            VeloxType::Row(fields) => {
                let field_strs: Vec<_> = fields.iter()
                    .map(|(name, ty)| format!("{} {}", name, ty.to_type_string()))
                    .collect();
                format!("ROW({})", field_strs.join(","))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_types() {
        assert_eq!(VeloxType::Boolean.to_type_string(), "BOOLEAN");
        assert_eq!(VeloxType::Integer.to_type_string(), "INTEGER");
        assert_eq!(VeloxType::Double.to_type_string(), "DOUBLE");
        assert_eq!(VeloxType::Varchar.to_type_string(), "VARCHAR");
    }

    #[test]
    fn test_array_type() {
        let arr = VeloxType::Array(Box::new(VeloxType::Integer));
        assert_eq!(arr.to_type_string(), "ARRAY<INTEGER>");
    }

    #[test]
    fn test_map_type() {
        let map = VeloxType::Map(
            Box::new(VeloxType::Varchar),
            Box::new(VeloxType::Integer),
        );
        assert_eq!(map.to_type_string(), "MAP<VARCHAR,INTEGER>");
    }
}
