use once_cell::sync::Lazy;
use tera::Tera;

#[allow(clippy::implicit_hasher)]
pub mod utils {
    use once_cell::sync::Lazy;
    use regex::Regex;
    use std::collections::HashMap;
    use std::io::BufRead;
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
        let cond = cond.as_bool().unwrap_or_else(|| !cond.is_null());
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

    pub fn progress(_args: &HashMap<String, Value>) -> Result<Value> {
        fn progress_res() -> Result<Value> {
            const MONTHS: [&str; 12] = [
                "January",
                "February",
                "March",
                "April",
                "May",
                "June",
                "July",
                "August",
                "September",
                "October",
                "November",
                "December",
            ];
            static DATE_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(\d\d\d\d)-(\d\d)").unwrap());

            let file =
                std::fs::File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/../ingest/todo.txt"))
                    .map_err(Error::msg)?;
            let mut buf_reader = std::io::BufReader::new(file);
            let mut line = String::new();

            buf_reader.read_line(&mut line).map_err(Error::msg)?;

            let caps = DATE_RE
                .captures(&line)
                .ok_or_else(|| Error::msg("No date matched in line"))?;

            Ok(format!(
                "{} of {}",
                MONTHS
                    .get(
                        caps.get(2)
                            .ok_or_else(|| Error::msg("No month captured in line"))?
                            .as_str()
                            .parse::<usize>()
                            .unwrap()
                            - 1
                    )
                    .ok_or_else(|| Error::msg("Month out of range"))?,
                caps.get(1)
                    .ok_or_else(|| Error::msg("No year captured in line"))?
                    .as_str(),
            )
            .into())
        }

        match progress_res() {
            Err(_e) => Ok(Value::Null),
            Ok(progress) => Ok(progress),
        }
    }
}

pub fn create_tera() -> Tera {
    match Tera::new(concat!(env!("CARGO_MANIFEST_DIR"), "/templates/*")) {
        Ok(mut t) => {
            t.register_filter("tern", utils::tern);
            t.register_filter("plural", utils::pluralize);
            t.register_tester("null", utils::null);
            t.register_function("progress", utils::progress);
            t
        }
        Err(e) => {
            println!("Parsing error(s): {}", e);
            std::process::exit(1);
        }
    }
}

pub static TERA: Lazy<Tera> = Lazy::new(create_tera);

#[macro_export]
macro_rules! get_tera {
    () => {{
        #[cfg(debug_assertions)]
        let tera = crate::render::create_tera();
        #[cfg(not(debug_assertions))]
        let tera = once_cell::sync::Lazy::force(&crate::render::TERA);

        tera
    }};
}
