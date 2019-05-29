use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

fn main() {
    let path = Path::new(&env::var("OUT_DIR").unwrap()).join("codegen.rs");
    let mut file = BufWriter::new(File::create(&path).unwrap());

    write!(
        &mut file,
        "static IMAGE_MIME_MAP: phf::Map<&'static str, image::ImageFormat> =
"
    )
    .unwrap();
    phf_codegen::Map::new()
        .entry("image/png", "image::ImageFormat::PNG")
        .entry("image/jpeg", "image::ImageFormat::JPEG")
        .entry("image/gif", "image::ImageFormat::GIF")
        .entry("image/webp", "image::ImageFormat::WEBP")
        .entry("image/x-portable-anymap", "image::ImageFormat::PNM")
        .entry("image/tiff", "image::ImageFormat::TIFF")
        .entry("image/x-targa", "image::ImageFormat::TGA")
        .entry("image/x-tga", "image::ImageFormat::TGA")
        .entry("image/bmp", "image::ImageFormat::BMP")
        .entry("image/vnd.microsoft.icon", "image::ImageFormat::ICO")
        .entry("image/vnd.radiance", "image::ImageFormat::HDR")
        .build(&mut file)
        .unwrap();
    write!(&mut file, ";\n").unwrap();
}
