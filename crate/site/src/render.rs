use once_cell::sync::Lazy;
use tera::Tera;

#[allow(clippy::implicit_hasher)]
pub mod utils {
    use std::collections::HashMap;
    use tera::{to_value, try_get_value, Error, Result, Value};

    pub fn pluralize(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
        let num = try_get_value!("pluralize", "value", f64, value);

        let plural = match args.get("plural") {
            Some(val) => try_get_value!("pluralize", "plural", String, val),
            None => String::from("s"),
        };

        let singular = match args.get("singular") {
            Some(val) => try_get_value!("pluralize", "singular", String, val),
            None => String::from(""),
        };

        // English uses plural when it isn't one
        if (num.abs() - 1.).abs() > std::f64::EPSILON {
            Ok(to_value(&plural).unwrap())
        } else {
            Ok(to_value(&singular).unwrap())
        }
    }

    pub fn tern(cond: &Value, args: &HashMap<String, Value>) -> Result<Value> {
        let cond = cond.as_bool().ok_or_else(|| Error::msg("Expected bool"))?;
        let yes = args
            .get("yes")
            .ok_or_else(|| Error::msg("Argument 'yes' missing"))?
            .clone();
        let no = args
            .get("no")
            .ok_or_else(|| Error::msg("Argument 'no' missing"))?
            .clone();
        Ok(if cond { yes } else { no })
    }

    pub fn null(arg: Option<&Value>, _args: &[Value]) -> Result<bool> {
        arg.ok_or_else(|| Error::msg("Tester `null` was called on an undefined variable"))
            .map(Value::is_null)
    }
}

pub fn create_tera() -> Tera {
    match Tera::new(concat!(env!("CARGO_MANIFEST_DIR"), "/templates/*")) {
        Ok(mut t) => {
            t.register_filter("tern", utils::tern);
            t.register_filter("plural", utils::pluralize);
            t.register_tester("null", utils::null);
            t
        }
        Err(e) => {
            println!("Parsing error(s): {}", e);
            std::process::exit(1);
        }
    }
}

pub static TERA: Lazy<Tera> = Lazy::new(create_tera);
