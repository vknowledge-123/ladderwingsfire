from pydantic import BaseModel, Field
from typing import Optional, List

class StrategySettings(BaseModel):
    # Credentials
    client_id: str = ""
    access_token: str = ""
    
    # Ladder Settings
    no_of_add_ons: int = 5
    add_on_percentage: float = 0.5  # % rise/fall to add more
    initial_stop_loss_pct: float = 2
    trailing_stop_loss_pct: float = 2
    target_percentage: float = 5
    
    # Selection Settings
    max_ladder_stocks: int = 10
    top_n_gainers: int = 5
    top_n_losers: int = 5
    min_turnover_crores: float = 1.0
    auto_start_market_open: bool = True

    # Entry Filters / Cycles
    max_open_gap_pct_long: float = 5.0
    min_open_gap_pct_short: float = -5.0
    cycles_per_stock: int = 3
    
    # Risk Management
    trade_capital: float = 1000.0
    profit_target_per_stock: float = 5000.0
    loss_limit_per_stock: float = 2000.0
    global_profit_exit: float = 4000.0
    global_loss_exit: float = 2000.0

    # Performance
    max_concurrent_orders: int = 10
    
    # State
    is_active: bool = False

class TradeSignal(BaseModel):
    symbol: str
    signal_type: str  # LONG, SHORT
    price: float
    timestamp: str

class StockStatus(BaseModel):
    symbol: str
    mode: str # LONG, SHORT, NONE (Closed)
    ltp: float
    change_pct: float
    pnl: float
    status: str # ACTIVE, CLOSED_PROFIT, CLOSED_LOSS, STOPPED, IDLE
    entry_price: float
    quantity: int
    ladder_level: int
    next_add_on: float
    stop_loss: float
    target: float
    prev_close: float = 0.0
    day_open: float = 0.0
    open_gap_pct: float = 0.0
    last_volume: float = 0.0  # Latest tick volume (as provided by Dhan feed)
    turnover: float = 0.0
    high_watermark: float = 0.0  # For trailing SL tracking
    order_ids: List[str] = Field(default_factory=list)  # Track all orders for this position
    avg_entry_price: float = 0.0  # Average entry price for accurate P&L
    pending_order: str = ""  # Tracks in-flight order intent (prevents duplicate orders)
    last_order_error: str = ""
    cycle_index: int = 0
    cycle_total: int = 1
    cycle_start_mode: str = ""

class PerformanceSettings(BaseModel):
    """Performance and optimization settings."""
    tick_batch_interval_ms: int = 100
    max_concurrent_orders: int = 2
    enable_performance_logging: bool = True
    websocket_reconnect_delay_seconds: int = 5
    order_retry_max_attempts: int = 3


