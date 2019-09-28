use super::{map_ue, UserError};
use image::{
    imageops, load_from_memory, DynamicImage, GenericImageView, GrayImage, ImageBgr8, ImageBgra8,
    ImageLuma8, ImageLumaA8, ImageRgb8, ImageRgba8,
};
use std::fmt::{self, Display, Formatter};
use tokio_postgres::{to_sql_checked, types};

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
        w: &mut Vec<u8>,
    ) -> Result<types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        (self.0 as i64).to_sql(t, w)
    }

    fn accepts(t: &types::Type) -> bool {
        i64::accepts(t)
    }

    to_sql_checked!();
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
        // match format {
        //     Some(format) => load_from_memory_with_format(&file, format),
        // None =>
        load_from_memory(&image)
            // }
            .map_err(map_ue!("invalid image"))?,
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

pub fn grayscale_simd(img: &DynamicImage) -> DynamicImage {
    use packed_simd::{shuffle, u32x16, u8x16, u8x64, FromCast};

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
        ImageRgb8(rgb) => ImageLuma8({
            let mut new_vec = vec![0; (width * height) as usize];

            let mut i = 0;
            for img_data in rgb.chunks(48) {
                let this_len = img_data.len();

                let mut data = [0; 64];
                data[..this_len].copy_from_slice(img_data);

                let data = unsafe { u8x64::from_slice_unaligned_unchecked(&data) };

                let reds: u8x16 = shuffle!(
                    data,
                    [0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39, 42, 45]
                );
                let greens: u8x16 = shuffle!(
                    data,
                    [1, 4, 7, 10, 13, 16, 19, 22, 25, 28, 31, 34, 37, 40, 43, 46]
                );
                let blues: u8x16 = shuffle!(
                    data,
                    [2, 5, 8, 11, 14, 17, 20, 23, 26, 29, 32, 35, 38, 41, 44, 47]
                );

                let mut reds = u32x16::from(reds);
                let mut greens = u32x16::from(greens);
                let mut blues = u32x16::from(blues);

                reds *= 2126;
                greens *= 7152;
                blues *= 722;

                let vals = u8x16::from_cast((reds + greens + blues) / 10000);

                let mut out = [0; 16];
                unsafe {
                    vals.write_to_slice_unaligned_unchecked(&mut out);
                }
                let out_len = this_len / 3;
                new_vec[i..i + out_len].copy_from_slice(&out[..out_len]);

                i += out_len;
            }

            GrayImage::from_vec(width, height, new_vec).unwrap()
        }),
        ImageRgba8(rgba) => ImageLuma8({
            let mut new_vec = vec![0; (width * height) as usize];

            let chunks_exact = rgba.chunks_exact(64);
            let remainder = chunks_exact.remainder();

            let mut i = 0;
            for img_data in chunks_exact {
                let data = u8x64::from_slice_unaligned(img_data);

                let reds: u8x16 = shuffle!(
                    data,
                    [0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60]
                );
                let greens: u8x16 = shuffle!(
                    data,
                    [1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 49, 53, 57, 61]
                );
                let blues: u8x16 = shuffle!(
                    data,
                    [2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42, 46, 50, 54, 58, 62]
                );

                let mut reds = u32x16::from(reds);
                let mut greens = u32x16::from(greens);
                let mut blues = u32x16::from(blues);

                reds *= 2126;
                greens *= 7152;
                blues *= 722;

                let vals = u8x16::from_cast((reds + greens + blues) / 10000);

                vals.write_to_slice_unaligned(new_vec.get_mut(i..i + 16).unwrap());

                i += 16;
            }

            for pixel in remainder.chunks(4) {
                new_vec[i] = rgb_to_luma(pixel[0], pixel[1], pixel[2]);
                i += 1;
            }

            GrayImage::from_vec(width, height, new_vec).unwrap()
        }),
        ImageBgr8(bgr) => ImageLuma8({
            let mut new_vec = vec![0; (width * height) as usize];

            let mut i = 0;
            for img_data in bgr.chunks(48) {
                let this_len = img_data.len();

                let mut data = [0; 64];
                data[..this_len].copy_from_slice(img_data);

                let data = unsafe { u8x64::from_slice_unaligned_unchecked(&data) };

                let blues: u8x16 = shuffle!(
                    data,
                    [0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39, 42, 45]
                );
                let greens: u8x16 = shuffle!(
                    data,
                    [1, 4, 7, 10, 13, 16, 19, 22, 25, 28, 31, 34, 37, 40, 43, 46]
                );
                let reds: u8x16 = shuffle!(
                    data,
                    [2, 5, 8, 11, 14, 17, 20, 23, 26, 29, 32, 35, 38, 41, 44, 47]
                );

                let mut blues = u32x16::from(blues);
                let mut greens = u32x16::from(greens);
                let mut reds = u32x16::from(reds);

                blues *= 722;
                greens *= 7152;
                reds *= 2126;

                let vals = u8x16::from_cast((blues + greens + reds) / 10000);

                let mut out = [0; 16];
                unsafe {
                    vals.write_to_slice_unaligned_unchecked(&mut out);
                }
                let out_len = this_len / 3;
                new_vec[i..i + out_len].copy_from_slice(&out[..out_len]);

                i += out_len;
            }

            GrayImage::from_vec(width, height, new_vec).unwrap()
        }),
        ImageBgra8(bgra) => ImageLuma8({
            let mut new_vec = vec![0; (width * height) as usize];

            let chunks_exact = bgra.chunks_exact(64);
            let remainder = chunks_exact.remainder();

            let mut i = 0;
            for img_data in chunks_exact {
                let data = u8x64::from_slice_unaligned(img_data);

                let blues: u8x16 = shuffle!(
                    data,
                    [0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60]
                );
                let greens: u8x16 = shuffle!(
                    data,
                    [1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 49, 53, 57, 61]
                );
                let reds: u8x16 = shuffle!(
                    data,
                    [2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42, 46, 50, 54, 58, 62]
                );

                let mut blues = u32x16::from(blues);
                let mut greens = u32x16::from(greens);
                let mut reds = u32x16::from(reds);

                blues *= 722;
                greens *= 7152;
                reds *= 2126;

                let vals = u8x16::from_cast((blues + greens + reds) / 10000);

                vals.write_to_slice_unaligned(new_vec.get_mut(i..i + 16).unwrap());

                i += 16;
            }

            for pixel in remainder.chunks(4) {
                new_vec[i] = rgb_to_luma(pixel[2], pixel[1], pixel[0]);
                i += 1;
            }

            GrayImage::from_vec(width, height, new_vec).unwrap()
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn grayscale_consistency() {
        for image_entry in Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("images")
            .read_dir()
            .unwrap()
        {
            let img = image::open(image_entry.unwrap().path()).unwrap();

            let gray1 = grayscale(&img);
            let gray2 = grayscale_simd(&img);

            assert_eq!(gray1.raw_pixels(), gray2.raw_pixels());
        }
    }
}
