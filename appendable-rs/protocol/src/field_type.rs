
/// `FieldType` represents the type of data stored in the field, which follows JSON types excluding Object and null. Object is broken down into subfields and null is not stored.
pub enum FieldType {
    String,
    Number,
    Object,
    Array,
    Boolean,
    Null,
}


/// `FieldFlags` is left as u64 to avoid shooting ourselves in the foot if we want to support more types in the future via other file formats
pub struct FieldFlags {
    flags: u64,
}

impl FieldFlags {
    pub fn new() -> Self {
        FieldFlags { flags: 0 }
    }

    pub fn set(&mut self, field: FieldType) {
        match field {
            FieldType::String => self.flags |= 1 << 0,
            FieldType::Number => self.flags |= 1 << 1,
            FieldType::Object => self.flags |= 1 << 2,
            FieldType::Array => self.flags |= 1 << 3,
            FieldType::Boolean => self.flags |= 1 << 4,
            FieldType::Null => self.flags |= 1 << 5,
        }
    }

    pub fn contains(&self, field: FieldType) -> bool {
        match field {
            FieldType::String => (self.flags & (1 << 0)) != 0,
            FieldType::Number => (self.flags & (1 << 1)) != 0,
            FieldType::Object => (self.flags & (1 << 2)) != 0,
            FieldType::Array => (self.flags & (1 << 3)) != 0,
            FieldType::Boolean => (self.flags & (1 << 4)) != 0,
            FieldType::Null => (self.flags & (1 << 5)) != 0,
        }
    }

    pub fn typescript_type(&self) -> String {
        let mut components = Vec::new();

        if self.contains(FieldType::String) {
            components.push("string");
        }

        if self.contains(FieldType::Number) {
            components.push("number");
        }

        if self.contains(FieldType::Object) {
            components.push("Record");
        }

        if self.contains(FieldType::Array) {
            components.push("any[]");
        }

        if self.contains(FieldType::Boolean) {
            components.push("boolean");
        }

        if self.contains(FieldType::Null) {
            components.push("null");
        }

        match components.is_empty() {
            true => "unknown".to_string(),
            false => components.join(" | ")
        }
    }
}
