use super::{map_ue_save, ue_save, Source, UserError};
use bytes::BytesMut;
use image::{imageops, load_from_memory, DynamicImage, GrayImage};
use std::fmt::{self, Display, Formatter};
use tokio_postgres::types;

#[derive(Debug, Copy, Clone)]
pub struct Hash(pub u64);

impl Display for Hash {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl types::ToSql for Hash {
    fn to_sql(
        &self,
        t: &types::Type,
        w: &mut BytesMut,
    ) -> Result<types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        (self.0 as i64).to_sql(t, w)
    }

    fn accepts(t: &types::Type) -> bool {
        i64::accepts(t)
    }

    types::to_sql_checked!();
}

pub fn dhash(img: DynamicImage) -> Result<Hash, UserError> {
    let small_img = imageops::thumbnail(&grayscale(&img)?, 9, 8);

    let mut hash: u64 = 0;

    for y in 0..8 {
        for x in 0..8 {
            let bit = ((small_img.get_pixel(x, y)[0] > small_img.get_pixel(x + 1, y)[0]) as u64)
                << (x + y * 8);
            hash |= bit;
        }
    }

    Ok(Hash(hash))
}

pub fn distance(a: Hash, b: Hash) -> u32 {
    (a.0 ^ b.0).count_ones()
}

pub fn hash_from_memory(image: &[u8]) -> Result<Hash, UserError> {
    dhash(load_from_memory(&image).map_err(map_ue_save!("invalid image", "image_invalid"))?)
}

fn rgb_to_luma(r: u8, g: u8, b: u8) -> u8 {
    ((u32::from(r) * 2126 + u32::from(g) * 7152 + u32::from(b) * 722) / 10000) as u8
}

pub fn grayscale(img: &DynamicImage) -> Result<DynamicImage, UserError> {
    let width = img.width();
    let height = img.height();

    use DynamicImage::*;
    Ok(ImageLuma8(match img {
        ImageLuma8(gray) => gray.clone(),
        ImageLumaA8(gray_alpha) => GrayImage::from_vec(
            width,
            height,
            gray_alpha.chunks_exact(2).map(|data| data[0]).collect(),
        )
        .unwrap(),
        ImageRgb8(rgb) => GrayImage::from_vec(
            width,
            height,
            rgb.chunks_exact(3)
                .map(|data| rgb_to_luma(data[0], data[1], data[2]))
                .collect(),
        )
        .unwrap(),
        ImageRgba8(rgba) => GrayImage::from_vec(
            width,
            height,
            rgba.chunks_exact(4)
                .map(|data| rgb_to_luma(data[0], data[1], data[2]))
                .collect(),
        )
        .unwrap(),
        _ => {
            return Err(ue_save!(
                "unsupported image color space",
                "image_color_space",
                Source::User
            ))
        }
    }))
}
