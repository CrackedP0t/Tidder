#![recursion_limit = "128"]

extern crate proc_macro;
use proc_macro::TokenStream;
use proc_macro2::{Ident, TokenStream as TokenStream2};
use syn::parse::{Parse, ParseStream, Result};
use syn::{parse_macro_input, LitInt, Token};

use quote::quote;

struct MultiEither {
    totals: Vec<LitInt>,
}

impl Parse for MultiEither {
    fn parse(input: ParseStream) -> Result<Self> {
        let totals = input
            .parse_terminated::<LitInt, Token![,]>(LitInt::parse)?
            .into_iter()
            .collect();
        Ok(MultiEither { totals })
    }
}

#[proc_macro]
pub fn multi_either(input: TokenStream) -> TokenStream {
    let MultiEither { totals } = parse_macro_input!(input as MultiEither);

    let out = totals.iter().fold(TokenStream2::new(), |mut out, total| {
        let total_value = total.value();

        let variants: Vec<_> = (1..=total_value)
            .map(|n| Ident::new(&format!("V{}", n), total.span()))
            .collect();
        let variants_r = &variants;

        let types: Vec<_> = (1..=total_value)
            .map(|n| Ident::new(&format!("T{}", n), total.span()))
            .collect();
        let types_r = &types;

        let generics = quote! {
            <#(#types_r),*>
        };

        let name = Ident::new(&format!("MultiEither{}", total_value), total.span());

        let skip_first = types.iter().skip(1);
        let name_rep = std::iter::repeat(&name);
        out.extend(quote! {
            pub enum #name #generics {
                #(#variants_r (#types_r)),*
            }

            impl #generics futures::Future for #name #generics
            where T1: futures::Future,
            #(#skip_first: futures::Future<Item = T1::Item, Error = T1::Error>),*
            {
                type Item = T1::Item;
                type Error = T1::Error;

                fn poll(&mut self) -> futures::Poll<T1::Item, T1::Error> {
                    match *self {
                        #(#name_rep::#variants_r(ref mut inner) => inner.poll()),*
                    }
                }
            }
        });

        out
    });

    out.into()
}
