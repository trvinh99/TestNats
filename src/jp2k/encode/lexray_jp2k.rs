use openjpeg_sys::*;
use serde::{Deserialize, Serialize};

use crate::aes;
use crate::jp2k::encode::support::*;
use crate::transcode_actor::FrameType;
use std::os::raw::c_void;
use std::ptr::null_mut;

// #[macro_use]
use slog::o;
use slog::Drain;

fn get_default_opj_poc() -> opj_poc {
    let poc = opj_poc {
        /// Resolution num start, Component num start, given by POC
        resno0: 0,
        /// Resolution num start, Component num start, given by POC
        compno0: 0,
        /// Layer num end,Resolution num end, Component num end, given by POC
        layno1: 0,
        /// Layer num end,Resolution num end, Component num end, given by POC
        resno1: 0,
        /// Layer num end,Resolution num end, Component num end, given by POC
        compno1: 0,
        /// Layer num start,Precinct num start, Precinct num end
        layno0: 0,
        /// Layer num start,Precinct num start, Precinct num end
        precno0: 0,
        /// Layer num start,Precinct num start, Precinct num end
        precno1: 0,
        /// Progression order enum
        prg1: PROG_ORDER::OPJ_PROG_UNKNOWN,
        /// Progression order enum
        prg: PROG_ORDER::OPJ_PROG_UNKNOWN,
        /// Progression order string
        progorder: [0; 5],
        /// Tile number (starting at 1)
        tile: 0,
        /// Start and end values for Tile width and height
        tx0: 0,
        /// Start and end values for Tile width and height
        tx1: 0,
        /// Start and end values for Tile width and height
        ty0: 0,
        /// Start and end values for Tile width and height
        ty1: 0,
        /// Start value, initialised in pi_initialise_encode
        layS: 0,
        /// Start value, initialised in pi_initialise_encode
        resS: 0,
        /// Start value, initialised in pi_initialise_encode
        compS: 0,
        /// Start value, initialised in pi_initialise_encode
        prcS: 0,
        /// End value, initialised in pi_initialise_encode
        layE: 0,
        /// End value, initialised in pi_initialise_encode
        resE: 0,
        /// End value, initialised in pi_initialise_encode
        compE: 0,
        /// End value, initialised in pi_initialise_encode
        prcE: 0,
        /// Start and end values of Tile width and height, initialised in pi_initialise_encode
        txS: 0,
        /// Start and end values of Tile width and height, initialised in pi_initialise_encode
        txE: 0,
        /// Start and end values of Tile width and height, initialised in pi_initialise_encode
        tyS: 0,
        /// Start and end values of Tile width and height, initialised in pi_initialise_encode
        tyE: 0,
        /// Start and end values of Tile width and height, initialised in pi_initialise_encode
        dx: 0,
        /// Start and end values of Tile width and height, initialised in pi_initialise_encode
        dy: 0,
        /// Temporary values for Tile parts, initialised in pi_create_encode
        lay_t: 0,
        /// Temporary values for Tile parts, initialised in pi_create_encode
        res_t: 0,
        /// Temporary values for Tile parts, initialised in pi_create_encode
        comp_t: 0,
        /// Temporary values for Tile parts, initialised in pi_create_encode
        prc_t: 0,
        /// Temporary values for Tile parts, initialised in pi_create_encode
        tx0_t: 0,
        /// Temporary values for Tile parts, initialised in pi_create_encode
        ty0_t: 0,
    };

    poc
}
unsafe fn get_default_encoder_parameters() -> opj_cparameters {
    let mut cp_matrice: i32 = 0;
    let mut cp_comment: i8 = 0;
    let poc = get_default_opj_poc();

    let mut jp2_cparams = opj_cparameters {
        tile_size_on: 0,
        /// XTOsiz
        cp_tx0: 0,
        /// YTOsiz
        cp_ty0: 0,
        /// XTsiz
        cp_tdx: 0,
        /// YTsiz
        cp_tdy: 0,
        /// allocation by rate/distortion
        cp_disto_alloc: 0,
        /// allocation by fixed layer
        cp_fixed_alloc: 0,
        /// add fixed_quality
        cp_fixed_quality: 0,
        /// fixed layer
        cp_matrice: &mut cp_matrice,
        /// comment for coding
        cp_comment: &mut cp_comment,
        /// csty : coding style
        csty: 0,
        /// progression order (default OPJ_LRCP)
        prog_order: PROG_ORDER::OPJ_PROG_UNKNOWN,
        /// progression order changes
        POC: [poc; 32],
        /// number of progression order changes (POC), default to 0
        numpocs: 0,
        /// number of layers
        tcp_numlayers: 0,
        /// rates of layers - might be subsequently limited by the max_cs_size field.
        /// Should be decreasing. 1 can be
        /// used as last value to indicate the last layer is lossless.
        tcp_rates: [0.0; 100],
        /// different psnr for successive layers. Should be increasing. 0 can be
        /// used as last value to indicate the last layer is lossless.
        tcp_distoratio: [0.0; 100],
        /// number of resolutions
        numresolution: 0,
        /// initial code block width, default to 64
        cblockw_init: 0,
        /// initial code block height, default to 64
        cblockh_init: 0,
        /// mode switch (cblk_style)
        mode: 0,
        /// 1 : use the irreversible DWT 9-7, 0 : use lossless compression (default)
        irreversible: 0,
        /// region of interest: affected component in [0..3], -1 means no ROI
        roi_compno: 0,
        /// region of interest: upshift value
        roi_shift: 0,
        res_spec: 0,
        /// initial precinct width
        prcw_init: [0; 33],
        /// initial precinct height
        prch_init: [0; 33],
        /// input file name
        infile: [0; 4096],
        /// output file name
        outfile: [0; 4096],
        /// DEPRECATED. Index generation is now handeld with the opj_encode_with_info() function. Set to NULL
        index_on: 0,
        /// DEPRECATED. Index generation is now handeld with the opj_encode_with_info() function. Set to NULL
        index: [0; 4096],
        /// subimage encoding: origin image offset in x direction
        image_offset_x0: 0,
        /// subimage encoding: origin image offset in y direction
        image_offset_y0: 0,
        /// subsampling value for dx
        subsampling_dx: 0,
        /// subsampling value for dy
        subsampling_dy: 0,
        /// input file format 0: PGX, 1: PxM, 2: BMP 3:TIF
        decod_format: 0,
        /// output file format 0: J2K, 1: JP2, 2: JPT
        cod_format: 1,
        /// enables writing of EPC in MH, thus activating JPWL
        jpwl_epc_on: 0,
        /// error protection method for MH (0,1,16,32,37-128)
        jpwl_hprot_MH: 0,
        /// tile number of header protection specification (>=0)
        jpwl_hprot_TPH_tileno: [0; 16],
        /// error protection methods for TPHs (0,1,16,32,37-128)
        jpwl_hprot_TPH: [0; 16],
        /// tile number of packet protection specification (>=0)
        jpwl_pprot_tileno: [0; 16],
        /// packet number of packet protection specification (>=0)
        jpwl_pprot_packno: [0; 16],
        /// error protection methods for packets (0,1,16,32,37-128)
        jpwl_pprot: [0; 16],
        /// enables writing of ESD, (0=no/1/2 bytes)
        jpwl_sens_size: 0,
        /// sensitivity addressing size (0=auto/2/4 bytes)
        jpwl_sens_addr: 0,
        /// sensitivity range (0-3)
        jpwl_sens_range: 0,
        /// sensitivity method for MH (-1=no,0-7)
        jpwl_sens_MH: 0,
        /// tile number of sensitivity specification (>=0)
        jpwl_sens_TPH_tileno: [0; 16],
        /// sensitivity methods for TPHs (-1=no,0-7)
        jpwl_sens_TPH: [0; 16],
        /// DEPRECATED: use RSIZ, OPJ_PROFILE_* and MAX_COMP_SIZE instead
        /// Digital Cinema compliance 0-not compliant, 1-compliant
        cp_cinema: CINEMA_MODE::OPJ_OFF,
        /// Maximum size (in bytes) for each component.
        /// If == 0, component size limitation is not considered
        max_comp_size: 0,
        /// DEPRECATED: use RSIZ, OPJ_PROFILE_* and OPJ_EXTENSION_* instead
        /// Profile name
        cp_rsiz: RSIZ_CAPABILITIES::OPJ_MCT,
        /// Tile part generation
        tp_on: 0,
        /// Flag for Tile part generation
        tp_flag: 0,
        /// MCT (multiple component transform)
        tcp_mct: 0,
        /// Enable JPIP indexing
        jpip_on: 0,
        /// Naive implementation of MCT restricted to a single reversible array based
        ///encoding without offset concerning all the components.
        mct_data: null_mut(),
        /// Maximum size (in bytes) for the whole codestream.
        /// If == 0, codestream size limitation is not considered
        /// If it does not comply with tcp_rates, max_cs_size prevails
        /// and a warning is issued.
        max_cs_size: 0,
        /// RSIZ value
        ///To be used to combine OPJ_PROFILE_*, OPJ_EXTENSION_* and (sub)levels values.
        rsiz: 0,
    };

    opj_set_default_encoder_parameters(&mut jp2_cparams);
    jp2_cparams
}

pub unsafe fn encode(frame: FrameItem) -> Option<EncodedFrame> {
    // let img_result = image::load_from_memory_with_format(&frame.image, ImageFormat::Jpeg);
    // let img = match img_result {
    //     Ok(image) => image,
    //     Err(_) => return None,
    // };
    // let dimentsion = img.dimensions();

    // let img16 = img.into_rgb8();
    let data = frame.image;

    // The dimensions method returns the images width and height.
    // println!!(&mut util_logger, "length {:?}", frame.image.len());
    // println!("length {:?}", data.len());
    // println!!(&mut util_logger, "dimentsion {:?}", dimentsion);
    // println!("dimentsion {:?}", dimentsion);
    let mut params = get_default_encoder_parameters();
    params.tcp_numlayers = 1;
    params.cp_fixed_quality = 1;
    params.tcp_rates[0] = 100.0;

    //The %quality parameter is the SNR.  The useful range is narrow:
    //  SNR < 27  (terrible quality)
    //   SNR = 34  (default; approximately equivalent to jpeg quality 75)
    //   SNR = 40  (very high quality)
    //   SNR = 45  (nearly lossless)
    //   Use 0 for default; 100 for lossless.
    params.tcp_distoratio[0] = 38.0;
    params.cp_tx0 = 0;
    params.cp_ty0 = 0;
    params.tile_size_on = 0;
    params.irreversible = 1;
    // params.numresolution = 1;
    params.prog_order = OPJ_PROG_ORDER::OPJ_LRCP;
    params.cp_disto_alloc = 0;
    params.cod_format = 1;
    let nv: Vec<char> = "lab.jp2".chars().collect();
    for i in 0..nv.len() {
        let _num = nv[i] as i8;
        // println!("num {:?}", num);

        params.outfile[i] = nv[i] as i8;
    }
    let codec = opj_create_compress(OPJ_CODEC_FORMAT::OPJ_CODEC_JP2);

    // TODO: What is actually a sensible key value pair here?
    let logger = get_logger().new(o!("function"=>"decode jpeg2000 stream"));
    let mut data_logger = LogHandlerData::new(logger.clone());
    let data_ptr: *mut LogHandlerData = &mut data_logger;
    let data_ptr = data_ptr as *mut c_void;
    opj_set_info_handler(codec, Some(info_handler), data_ptr);
    opj_set_warning_handler(codec, Some(warning_handler), data_ptr);
    opj_set_error_handler(codec, Some(error_handler), data_ptr);

    let subsampling_dx = params.subsampling_dx as u32;
    let subsampling_dy = params.subsampling_dy as u32;
    let numcomps = 3;
    let prec = 8; //TODO: allow other bitdepths!
    let w = frame.frame_width as u32;
    let h = frame.frame_height as u32;
    params.tcp_mct = 1;
    // println!("index {:?}, value {:?}", subsampling_dx, subsampling_dy);

    let mut opj_image_comptparm_array: [opj_image_comptparm; 3] = [opj_image_comptparm {
        /// XRsiz: horizontal separation of a sample of ith component with respect to the reference grid
        dx: subsampling_dx,
        /// YRsiz: vertical separation of a sample of ith component with respect to the reference grid
        dy: subsampling_dy,
        /// data width
        w: w,
        /// data height
        h: h,
        /// x component offset compared to the whole image
        x0: 0,
        /// y component offset compared to the whole image
        y0: 0,
        /// precision
        prec: prec,
        /// image depth in bits
        bpp: prec,
        /// signed (1) / unsigned (0)
        sgnd: 0,
    }; 3];
    let image = opj_image_create(
        numcomps,
        &mut opj_image_comptparm_array[0],
        OPJ_COLOR_SPACE::OPJ_CLRSPC_SRGB,
    );
    (*image).x0 = params.image_offset_x0 as u32;
    (*image).y0 = params.image_offset_y0 as u32;
    (*image).x1 = params.image_offset_x0 as u32 + (w - 1) * params.subsampling_dx as u32 + 1;
    (*image).y1 = params.image_offset_y0 as u32 + (h - 1) * params.subsampling_dy as u32 + 1;
    (*image).color_space = OPJ_COLOR_SPACE::OPJ_CLRSPC_SRGB;

    for i in 0..(w * h) {
        for k in 0..numcomps {
            let index = (i * 3 + k) as usize;
            let comps = (*image).comps;
            let comp_data = comps.offset(k as isize);

            let value = data[index];
            //println!("index {:?}, value {:?}",index, value);
            *(*comp_data).data.offset(i as isize) = value as i32;
        }
    }
    println!("setup encoder");
    // println!("setup encoder");

    let b_success = opj_setup_encoder(codec, &mut params, image);
    if b_success == 1 {
        println!("ok");
        // println!("ok");
    } else {
        println!("failed");
        // println!("failed");
    }
    opj_codec_set_threads(codec, 4);
    let mut userdata = NdUserdata::new_output(&[0]);
    let userdata_ptr: *mut NdUserdata = &mut userdata;
    let stream = opj_stream_create(1048576, 0);
    //let f = CString::new("lab.jp2").expect("CString::new failed");
    //let stream = opj_stream_default_create(0);
    // let stream = opj_stream_create_default_file_stream(f.as_ptr(), 0);

    //opj_stream_set_read_function(stream,Some(nd_opj_stream_read_fn));
    opj_stream_set_seek_function(stream, Some(nd_opj_stream_seek_fn));
    opj_stream_set_skip_function(stream, Some(nd_opj_stream_skip_fn));
    opj_stream_set_write_function(stream, Some(nd_opj_stream_write_fn));
    opj_stream_set_user_data(stream, userdata_ptr as *mut c_void, None);
    opj_stream_set_user_data_length(stream, 0);

    println!("encode");

    opj_start_compress(codec, image, stream);
    let d = opj_encode(codec, stream);
    if d == 1 {
        println!("ok");
    } else {
        println!("failed");
    }
    opj_end_compress(codec, stream);

    opj_destroy_codec(codec);
    opj_image_destroy(image);
    opj_stream_destroy(stream);
    let iv: [u8; 16] = [
        99, 10, 152, 128, 166, 26, 133, 191, 249, 38, 14, 167, 122, 46, 4, 129,
    ];

    // let mut buffer = File::create("foo2.jp2").unwrap();

    // let reference = buffer.by_ref();

    // // we can use reference just like our original buffer
    // reference.write_all(&userdata.output);
    println!("[ENCODE] SESSION KEY: {:?}", frame.session_key.clone());

    let encrypted_frame = aes::encrypt(&userdata.output, &frame.session_key, &iv);
    match encrypted_frame {
        Ok(encoded_frame) => Option::Some(EncodedFrame {
            image: encoded_frame,
            timestamp: frame.timestamp,
            frame_type: "j2k".to_string(),
            camera_id: String::from_utf8(frame.camera_id.clone()).unwrap_or_default(),
        }),
        Err(_) => Option::None,
    }

    // let mut buffer = File::create("foo2.jp2").unwrap();

    // let reference = buffer.by_ref();

    // // we can use reference just like our original buffer
    // reference.write_all(&userdata.output);
    // match d {
    //     Ok(_) => Option::None,
    //     Err(_) => Option::None,
    // }
}

fn get_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    slog::Logger::root(drain, o!())
}

#[derive(Debug, Clone)]
pub struct FrameItem {
    pub image: Vec<u8>,
    pub timestamp: u128,
    pub session_key: Vec<u8>,
    pub camera_id: Vec<u8>,
    pub frame_type: FrameType,
    pub frame_width: usize,
    pub frame_height: usize,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EncodedFrame {
    pub image: Vec<u8>,
    pub timestamp: u128,
    pub frame_type: String,
    pub camera_id: String,
}
