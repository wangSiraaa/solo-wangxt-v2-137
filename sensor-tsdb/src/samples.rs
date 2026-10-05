//! 内置样本集：不同采样间隔、常值段、突变与特殊值。
//! 所有样本经 POST /api/seed 走「写入 → 读回 → 逐位核对」的完整闭环。

use crate::segment::ValueCodec;

pub struct SampleSpec {
    pub name: &'static str,
    pub codec: ValueCodec,
    pub points: Vec<(i64, u64)>,
}

/// 固定起点（毫秒时间戳），保证 seed 可重复、可核对。
const BASE: i64 = 1_700_000_000_000;

pub fn generate() -> Vec<SampleSpec> {
    vec![sine_1ms(), const_100ms(), const_100ms_raw(), step_1s(), random_10ms_raw()]
}

/// 1ms 采样：平滑正弦，XOR 压缩的典型场景。
fn sine_1ms() -> SampleSpec {
    let points = (0..10_000)
        .map(|i| {
            let ts = BASE + i as i64; // 1ms 间隔
            let phase = i as f64 * 0.01;
            (ts, phase.sin().to_bits())
        })
        .collect();
    SampleSpec { name: "sample-1ms-sine", codec: ValueCodec::Xor, points }
}

/// 100ms 采样：常值段。XOR 下每个点仅 1 bit，验证常值压缩与逐位还原。
fn const_100ms() -> SampleSpec {
    let points = (0..5_000)
        .map(|i| (BASE + i as i64 * 100, 25.0f64.to_bits()))
        .collect();
    SampleSpec { name: "sample-100ms-const", codec: ValueCodec::Xor, points }
}

/// 同样的常值数据用 raw（按位保存）编码：压缩率对照组。
fn const_100ms_raw() -> SampleSpec {
    let points = (0..5_000)
        .map(|i| (BASE + i as i64 * 100, 25.0f64.to_bits()))
        .collect();
    SampleSpec { name: "sample-100ms-const-raw", codec: ValueCodec::Raw, points }
}

/// 1s 采样：突变（阶跃）+ 特殊值。NaN / -0.0 / ±∞ / 次正规数必须逐位还原。
fn step_1s() -> SampleSpec {
    let mut points = Vec::with_capacity(3_600);
    for i in 0..3_600i64 {
        let ts = BASE + i * 1_000;
        // 两次突变：20.0 -> 87.5 -> -13.25
        let v: f64 = if i < 1_200 {
            20.0
        } else if i < 2_400 {
            87.5
        } else {
            -13.25
        };
        points.push((ts, v.to_bits()));
    }
    // 在已知位置注入特殊值
    points[100].1 = f64::NAN.to_bits(); // 0x7ff8000000000000 安静 NaN
    points[200].1 = (-0.0f64).to_bits(); // 0x8000000000000000 负零
    points[300].1 = f64::INFINITY.to_bits(); // +inf
    points[400].1 = f64::NEG_INFINITY.to_bits(); // -inf
    points[500].1 = 0x7ff8_0000_0000_0001; // 带负载 NaN
    points[600].1 = 0x0000_0000_0000_0001; // 最小次正规数
    SampleSpec { name: "sample-1s-step", codec: ValueCodec::Xor, points }
}

/// 10ms 采样：raw 编码 + 确定性伪随机位模式，验证 raw 路径逐位一致。
fn random_10ms_raw() -> SampleSpec {
    let mut s = 0x9e37_79b9_7f4a_7c15u64;
    let points = (0..2_000)
        .map(|i| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            (BASE + i as i64 * 10, s)
        })
        .collect();
    SampleSpec { name: "sample-10ms-random-raw", codec: ValueCodec::Raw, points }
}
