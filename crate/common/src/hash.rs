use super::{map_ue_save, UserError};
use image::{
    imageops, load_from_memory, DynamicImage, GenericImageView, GrayImage, ImageBgr8, ImageBgra8,
    ImageLuma8, ImageLumaA8, ImageRgb8, ImageRgba8,
};
use std::fmt::{self, Display, Formatter};
use tokio_postgres::types;
use bytes::BytesMut;

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

pub fn dhash(img: DynamicImage) -> Hash {
    let small_img = imageops::thumbnail(&grayscale(&img), 9, 8);

    let mut hash: u64 = 0;

    for y in 0..8 {
        for x in 0..8 {
            let bit = ((small_img.get_pixel(x, y)[0] > small_img.get_pixel(x + 1, y)[0]) as u64)
                << (x + y * 8);
            hash |= bit;
        }
    }

    Hash(hash)
}

pub fn distance(a: Hash, b: Hash) -> u32 {
    (a.0 ^ b.0).count_ones()
}

pub fn hash_from_memory(image: &[u8]) -> Result<Hash, UserError> {
    Ok(dhash(
        load_from_memory(&image).map_err(map_ue_save!("invalid image", "image_invalid"))?,
    ))
}

fn rgb_to_luma(r: u8, g: u8, b: u8) -> u8 {
    ((u32::from(r) * 2126 + u32::from(g) * 7152 + u32::from(b) * 722) / 10000) as u8
}

pub fn grayscale(img: &DynamicImage) -> DynamicImage {
    let width = img.width();
    let height = img.height();
    match img {
        ImageLuma8(gray) => ImageLuma8(gray.clone()),
        ImageLumaA8(gray_alpha) => ImageLuma8(
            GrayImage::from_vec(
                width,
                height,
                gray_alpha.chunks(2).map(|data| data[0]).collect(),
            )
            .unwrap(),
        ),
        ImageRgb8(rgb) => ImageLuma8(
            GrayImage::from_vec(
                width,
                height,
                rgb.chunks(3)
                    .map(|data| rgb_to_luma(data[0], data[1], data[2]))
                    .collect(),
            )
            .unwrap(),
        ),
        ImageRgba8(rgba) => ImageLuma8(
            GrayImage::from_vec(
                width,
                height,
                rgba.chunks(4)
                    .map(|data| rgb_to_luma(data[0], data[1], data[2]))
                    .collect(),
            )
            .unwrap(),
        ),
        ImageBgr8(bgr) => ImageLuma8(
            GrayImage::from_vec(
                width,
                height,
                bgr.chunks(3)
                    .map(|data| rgb_to_luma(data[2], data[1], data[0]))
                    .collect(),
            )
            .unwrap(),
        ),
        ImageBgra8(bgra) => ImageLuma8(
            GrayImage::from_vec(
                width,
                height,
                bgra.chunks(4)
                    .map(|data| rgb_to_luma(data[2], data[1], data[0]))
                    .collect(),
            )
            .unwrap(),
        ),
    }
}
