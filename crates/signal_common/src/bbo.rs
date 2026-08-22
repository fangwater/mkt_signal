#[derive(Debug, Clone, Copy, Default)]
pub struct Bbo {
    pub bid0: f64,
    pub ask0: f64,
    pub ts: i64,
}

impl Bbo {
    pub fn new(bid0: f64, ask0: f64, ts: i64) -> Self {
        Self { bid0, ask0, ts }
    }

    pub fn update(&mut self, bid0: f64, ask0: f64, ts: i64) {
        self.bid0 = bid0;
        self.ask0 = ask0;
        self.ts = ts;
    }

    pub fn is_valid(&self) -> bool {
        self.bid0 > 0.0 && self.ask0 > 0.0
    }

    pub fn mid_price(&self) -> Option<f64> {
        self.get_mid_price()
    }

    pub fn get_mid_price(&self) -> Option<f64> {
        if self.bid0 > 0.0 && self.ask0 > 0.0 {
            return Some((self.bid0 + self.ask0) * 0.5);
        }
        if self.bid0 > 0.0 {
            return Some(self.bid0);
        }
        if self.ask0 > 0.0 {
            return Some(self.ask0);
        }
        None
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DualBbo {
    pub open: Bbo,
    pub hedge: Bbo,
}

impl DualBbo {
    pub fn new(open: Bbo, hedge: Bbo) -> Self {
        Self { open, hedge }
    }

    pub fn open_mid(&self) -> Option<f64> {
        self.open.mid_price()
    }

    pub fn hedge_mid(&self) -> Option<f64> {
        self.hedge.mid_price()
    }
}
