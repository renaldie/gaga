# %% [markdown]
# # Data Loading

# %%
# %pip install -qq polars

# %%
import polars as pl
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time

# %% [markdown]
# ## Load from Directory

# %%
directory = "D:/Downloads/Gagaolala/Original/FillNA/"

# Read the txn data
txn_df = pl.read_csv(
    f"{directory}txn_v2processed.csv",
    schema_overrides={
        'id': pl.UInt32,
        'user_id': pl.UInt32,
        'type_no': pl.UInt8,
        'plan_id': pl.Float64,
        'ios_txnid': pl.String,
        'and_orderid': pl.String,
        'paypal_subscriptionid': pl.String,
        'payment_method': pl.Float64,
        'amount': pl.Float32,  # for decimal(6,2)
        'refund_state': pl.Categorical,
        'invoice_state': pl.Categorical,
        'promo_code': pl.String,
        'state': pl.Categorical,
        'created': pl.Int32,
        'updated': pl.Int32,
        'refunded': pl.Float32,
        'ip': pl.String,
        'amazon_receiptid': pl.String
    }
).with_columns([
    pl.from_epoch(pl.col(['created', 'updated', 'refunded'])).cast(pl.Date),
    pl.col(['plan_id', 'payment_method']).cast(pl.UInt8)
])

# # Read the plan data
# plan_df = pl.read_csv(
#     f"{directory}plan.csv",
#     schema_overrides={
#         'id': pl.UInt8,
#         'name_tc': pl.String,
#         'name_sc': pl.String,
#         'name_en': pl.String,
#         'avails_csv': pl.String,
#         'price_full_en': pl.String,
#         'price_full_tc': pl.String,
#         'price_full_sc': pl.String,
#         'price': pl.UInt16,
#         'price_usd': pl.Float32,
#         'period': pl.Categorical,
#         'is_app': pl.UInt8,
#         'is_paypal': pl.UInt8,
#         'is_tappay': pl.UInt8,
#         'is_newebpay': pl.UInt8,
#         'is_subscription': pl.UInt8,
#         'ios_productid': pl.String,
#         'and_productid': pl.String,
#         'paypal_plan_en': pl.String,
#         'paypal_plan_tc': pl.String,
#         'paypal_plan_sc': pl.String,
#         'days': pl.UInt16,
#         'bonus_days': pl.UInt8,
#         'date_start': pl.Date,
#         'date_end': pl.Date,
#         'ts_start': pl.UInt8,
#         'ts_end': pl.UInt8,
#         'created': pl.Int32,
#         'updated': pl.Int32,
#         'creator_admin_id': pl.UInt8,
#         'updater_admin_id': pl.UInt8,
#         'state': pl.Categorical,
#         'amazon_productid': pl.String
#     }
# ).with_columns([
#     pl.from_epoch(pl.col(['created', 'updated'])).cast(pl.Date),
#     pl.col(['is_app', 'is_paypal', 'is_tappay', 'is_newebpay', 'is_subscription']).cast(pl.Boolean),
#     pl.col('price_usd').cast(pl.Float32).round(2),
#     pl.col('bonus_days').cast(pl.UInt8)
# ])

# Read the user_video data to know the user's bookmark history
user_video_df = pl.read_csv(
    f"{directory}user_video.csv",
    schema_overrides={
        'user_id': pl.UInt32,
        'video_id': pl.UInt32,
        'bookmarked': pl.Int64
    }
).with_columns([
    pl.from_epoch(pl.col('bookmarked')).cast(pl.Date)
])

# Read the log_user_watch_daily data to know the user's watch history
log_user_watch_daily_df = pl.read_csv(
    f"{directory}log_user_watch_daily.csv",
    schema_overrides={
        'id': pl.UInt64,
        'date': pl.Date,
        'user_id': pl.UInt32,
        'video_id': pl.UInt32,
        'playtime_pct': pl.Float32,
        'max_playtime_pct': pl.Float32,
        'total_elapsed': pl.UInt32,
        'is_trial': pl.UInt8,
        'created': pl.Int64,
        'updated': pl.Int64
    }
).with_columns([
    pl.from_epoch(pl.col(['created', 'updated'])).cast(pl.Date),
    pl.col('is_trial').cast(pl.Boolean)
])

# # Read the category data to know the available movie categories in the platform ("entertainment", "shorts", "features", "originals", "series")
# category_df = pl.read_csv(
#     f"{directory}category.csv",
#     schema_overrides={
#         'id': pl.UInt8, # Main column
#         'parent_id': pl.UInt8,
#         'slug': pl.String, # Main column
#         'is_series': pl.UInt8,
#         'name_en': pl.String,
#         'name_tc': pl.String,
#         'name_sc': pl.String,
#         'description_en': pl.String,
#         'description_tc': pl.String,
#         'description_sc': pl.String,
#         'cover': pl.String,
#         'ordering': pl.UInt8,
#         'created': pl.Int32,
#         'updated': pl.Int32,
#         'creator_admin_id': pl.UInt8,
#         'updater_admin_id': pl.UInt8,
#         'state': pl.Categorical
#     }
# ).with_columns([
#     pl.from_epoch(pl.col(['created', 'updated'])).cast(pl.Date),
#     pl.col('is_series').cast(pl.Boolean)
# ])

# # Read the video_category data
# video_category_df = pl.read_csv(
#     f"{directory}video_category.csv",
#     schema_overrides={
#         'id': pl.UInt8,
#         'slug': pl.String,
#         'is_series': pl.UInt8,
#         'name_en': pl.String,
#         'name_tc': pl.String,
#         'name_sc': pl.String,
#         'description_en': pl.String,
#         'description_tc': pl.String,
#         'description_sc': pl.String,
#         'cover': pl.String,
#         'ordering': pl.UInt8,
#         'created': pl.Int32,
#         'updated': pl.Int32,
#         'creator_admin_id': pl.UInt8,
#         'updater_admin_id': pl.UInt8,
#         'state': pl.Categorical
#     }
# ).with_columns([
#     pl.from_epoch(pl.col(['created', 'updated'])).cast(pl.Date),
#     pl.col('is_series').cast(pl.Boolean)
# ])

# Read the video_category_rel data
video_category_rel_df = pl.read_csv(
    f"{directory}video_category_rel.csv",
    schema_overrides={
        'video_id': pl.UInt32,
        'video_category_id': pl.UInt8
    }
)

# Read the video / content data
video_df = pl.read_csv(
    f"{directory}video.csv",
    schema_overrides={
        'id': pl.UInt32,
        'media_id': pl.Float64,
        'category_id': pl.Float32,
        'year_released': pl.Float32,
        'country_en': pl.String,
        'country_tc': pl.String,
        'country_sc': pl.String,
        'publisher': pl.String,
        'classification': pl.String,
        'duration_min': pl.Float32,
        'is_free': pl.UInt8,
        'is_preview': pl.UInt8,
        'is_xc': pl.UInt8,
        'is_series': pl.UInt8,
        'is_applinks': pl.UInt8,
        'is_ads_18': pl.UInt8,
        'season': pl.Float32,
        'episode': pl.Float32,
        'slug': pl.String,
        'name_local': pl.String,
        'name_en': pl.String,
        'name_tc': pl.String,
        'name_sc': pl.String,
        'description_en': pl.String,
        'description_tc': pl.String,
        'description_sc': pl.String,
        'langs_en': pl.String,
        'langs_tc': pl.String,
        'langs_sc': pl.String,
        'director_en': pl.String,
        'director_tc': pl.String,
        'director_sc': pl.String,
        'actor_en': pl.String,
        'actor_tc': pl.String,
        'actor_sc': pl.String,
        'awards_en': pl.String,
        'awards_tc': pl.String,
        'awards_sc': pl.String,
        'review_url_en': pl.String,
        'review_title_en': pl.String,
        'review_url_tc': pl.String,
        'review_title_tc': pl.String,
        'review_url_sc': pl.String,
        'review_title_sc': pl.String,
        'subtitles': pl.String,
        'trailer_url': pl.String,
        'date_start': pl.Date,
        'date_end': pl.Date,
        'ts_start': pl.Float32,
        'ts_end': pl.Float32,
        'tags_en': pl.String,
        'tags_tc': pl.String,
        'tags_sc': pl.String,
        'avail_tw': pl.UInt8,
        'avail_hk': pl.UInt8,
        'avail_sg': pl.UInt8,
        'avail_bn': pl.UInt8,
        'avail_kh': pl.UInt8,
        'avail_id': pl.UInt8,
        'avail_la': pl.UInt8,
        'avail_my': pl.UInt8,
        'avail_mm': pl.UInt8,
        'avail_mo': pl.UInt8,
        'avail_ph': pl.UInt8,
        'avail_th': pl.UInt8,
        'avail_vn': pl.UInt8,
        'avail_af': pl.UInt8,
        'avail_bd': pl.UInt8,
        'avail_bt': pl.UInt8,
        'avail_in': pl.UInt8,
        'avail_mv': pl.UInt8,
        'avail_np': pl.UInt8,
        'avail_pk': pl.UInt8,
        'avail_lk': pl.UInt8,
        'covers': pl.String,
        'pics': pl.String,
        'trailers_json': pl.String,
        'rating': pl.Float32,
        'pop_score': pl.Float32,
        'created': pl.Int32,
        'updated': pl.Int32,
        'creator_admin_id': pl.UInt8,
        'updater_admin_id': pl.UInt8,
        'state': pl.Categorical
    }
).with_columns([
    pl.from_epoch(pl.col(['ts_start', 'ts_end', 'created', 'updated'])).cast(pl.Date),
    pl.col([
        'is_free', 'is_preview', 'is_xc', 'is_series', 'is_applinks', 'is_ads_18',
        'avail_tw', 'avail_hk', 'avail_sg', 'avail_bn', 'avail_kh', 'avail_id',
        'avail_la', 'avail_my', 'avail_mm', 'avail_mo', 'avail_ph', 'avail_th',
        'avail_vn', 'avail_af', 'avail_bd', 'avail_bt', 'avail_in', 'avail_mv',
        'avail_np', 'avail_pk', 'avail_lk'
    ]).cast(pl.Boolean),
    pl.col('category_id').cast(pl.UInt8)
])

# Read the user data
user_df = pl.read_csv(
    f"{directory}user.csv",
    schema_overrides={
        'id': pl.UInt32,
        'sns_no': pl.UInt8,
        'lang': pl.Categorical,
        'payment_method': pl.UInt8,
        'plan_id': pl.UInt8,
        'is_auto_renew': pl.UInt8,
        'next_pay_date': pl.Date,
        'created': pl.Int32,
        'updated': pl.Int32,
        'authed': pl.Float64,
        'email_confirmed': pl.Int32,
        'email_verified': pl.Float64,
        'mobile_verified': pl.Float64,
        'state': pl.Categorical
    }
).with_columns([
    pl.from_epoch(pl.col(['created', 'updated', 'authed', 'email_confirmed', 'email_verified', 'mobile_verified'])).cast(pl.Date),
    pl.col(['is_auto_renew']).cast(pl.Boolean),
    pl.col('next_pay_date').cast(pl.Date)
])

# %% [markdown]
# ## Create manual DF for small table

# %%
plan = '''
id,name_en,period,days
1,Monthly Subscription,M,0
2,Monthly Subscription,M,0
3,1-Month Plan,P,30
4,2-Month Plan,P,60
5,Seasonal Pass (10% off),P,90
6,Monthly Subscription (APP),M,0
7,Monthly Subscription,M,0
8,Monthly Subscription,M,0
9,Monthly Subscription,M,0
10,180-day Plan (17% off ),P,180
11,360-day Plan (25% off),P,360
12,Quarterly Subscription (10% off),3M,0
13,Semi-Annual Subscription (20% off),6M,0
14,Annual Subscription (30% off),Y,0
15,Quarterly Subscription (10% off),3M,0
16,Semi-Annual Subscription (20% off),6M,0
17,Annual Subscription (30% off),Y,0
18,Monthly Subscription,M,0
19,Quarterly Subscription (10% off),3M,0
20,Semi-Annual Subscription (20% off),6M,0
21,Annual Subscription (30% off),Y,0
22,Monthly Subscription,M,0
23,Quarterly Subscription (10% off),3M,0
24,Semi-Annual Subscription (20% off),6M,0
25,Annual Subscription (30% off),Y,0
26,,M,0
27,,3M,0
28,,6M,0
29,,Y,0
30,,Y,0
31,,P,360
32,,Y,0
33,,Y,0
34,,Y,0
35,Monthly Subscription,M,0
'''
# Create plan_df
plan_df = pl.read_csv(
    plan.encode(),
    schema={
        "id": pl.UInt8,
        "name_en": pl.Utf8,
        "period": pl.Categorical,
        "days": pl.UInt16
    }
)

# Create category_df to for video category reference table
category_df = pl.DataFrame({"id": [1, 2, 3, 4, 5],
                            "slug": ["entertainment", "shorts", "features", "originals", "series"]
                            }, 
                            schema=[("id", pl.UInt8), 
                                    ("slug", pl.String)])

# Create video_category_df to for video genre reference table
video_category_df = pl.DataFrame({"id": [1, 2, 3, 4, 5],
                                  "slug": ["gay", "lesbian", "queer", "bl", "free"]
                                  },
                                  schema=[("id", pl.UInt8), 
                                          ("slug", pl.String)])

# %% [markdown]
# # Set Limit and Tolerance

# %%
# Set the cutoff date
CUTOFF_DATE = txn_df.sort('id').select('created').tail(1).item()
# print(CUTOFF_DATE)

# Set churn tolerance
churn_month_offset = '1mo'
churn_day_offset = '5d'

# %% [markdown]
# # Preprocessing

# %% [markdown]
# ## Filter only necessary columns

# %%
video_df = video_df.select(['id', 'category_id', 'slug'])
# print(video_df)

user_df = user_df.select(['id', 'sns_no', 'country', 'is_auto_renew', 'email_verified', 'mobile_verified', 'state'])
# print(user_df)

# %% [markdown]
# ## Column Joining

# %% [markdown]
# ### `video_df` and `category_df` to the video category of features, short, etc.

# %%
video_df = video_df.join(category_df, 
              left_on='category_id', 
              right_on='id',
              how='left')
video_df.drop_in_place('category_id')
video_df = video_df.rename({'slug': 'video_name',
                            'slug_right': 'video_category'})
# video_df.head()

# %% [markdown]
# ### `video_category_rel_df` and `video_category_df` to define video genre

# %%
video_category_rel_df = video_category_rel_df.join(video_category_df,
                           left_on='video_category_id',
                           right_on='id',
                           how='left')
video_category_rel_df.drop_in_place('video_category_id')
video_category_rel_df = video_category_rel_df.rename({'slug': 'video_genre'})
# video_category_rel_df.head()

# %% [markdown]
# ### `video_df` and `video_category_rel_df` to define video name and video genre

# %%
video_df = video_df.join(video_category_rel_df,
              left_on='id',
              right_on='video_id',
              how='left')
# video_df.head()

# %% [markdown]
# ### Pivot to wide format, change genre and category to boolean numerical

# %%
video_df = (video_df.pivot(
    on='video_genre',       
    values='video_genre',
    index=['id', 'video_name', 'video_category'],
    aggregate_function='len',
    sort_columns=True
)
.rename(
    {**{col: f'video_genre_{col}' for col in video_df['video_genre'].unique().drop_nulls()},
     'null': 'video_genre_null'}
)
.fill_null(0)
)

video_df = (video_df.pivot(
    on='video_category',       
    values='video_category',
    index=['id', 'video_name', 'video_genre_bl', 'video_genre_free', 'video_genre_gay', 'video_genre_lesbian', 'video_genre_null', 'video_genre_queer'],
    aggregate_function='len',
    sort_columns=True
)
.rename(
    {**{col: f'video_category_{col}' for col in video_df['video_category'].unique().drop_nulls()},
     'null': 'video_category_null'}
)
.fill_null(0)
)

video_df = video_df.with_columns([
    pl.col([
        'video_genre_bl',
        'video_genre_free',
        'video_genre_gay',
        'video_genre_lesbian',
        'video_genre_null',
        'video_genre_queer',
        'video_category_entertainment',
        'video_category_features',
        'video_category_null',
        'video_category_originals',
        'video_category_series',
        'video_category_shorts'
    ]).cast(pl.Boolean)
])

# video_df.head()

# %% [markdown]
# ### `log_user_watch_daily_df` and `video_df` to define the details of watched video

# %%
log_user_watch_daily_df = log_user_watch_daily_df.join(video_df,
                                                       left_on='video_id',
                                                       right_on='id',
                                                       how='left')
# log_user_watch_daily_df.head()

# %% [markdown]
# ### `txn` and `user` to define user details in each txn

# %%
txn_df = (txn_df
        .join(user_df,
            left_on='user_id',
            right_on='id',
            how='left')
        .with_columns([
            pl.when(pl.col('email_verified') < pl.col('created'))
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .cast(pl.Boolean)
            .alias('is_email_verified'),

            pl.when(pl.col('mobile_verified') < pl.col('created'))
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .cast(pl.Boolean)
            .alias('is_mobile_verified')
            ])
        .rename({'state_right': 'user_state',  
                'state': 'txn_state'})
        .drop(['email_verified', 'mobile_verified'])
        )
# txn_df

# %% [markdown]
# ## Defining the Churn

# %%
# Create subscription types mapping
subscription_types = {
    'monthly': [1, 2, 3, 6, 7, 8, 9, 18, 22, 26, 35],
    'bimonthly': [4],
    'quarterly': [5, 12, 15, 19, 23, 27],
    'semi-annual': [10, 13, 16, 20, 24, 28],
    'annually': [11, 14, 17, 21, 25, 29, 30, 31, 32, 33, 34]
}

# Create function to get subscription type
def get_subscription_type(plan_id):
    if plan_id == 0 or plan_id is None:
        return 'free'
    for sub_type, plan_ids in subscription_types.items():
        if plan_id in plan_ids:
            return sub_type
    return 'unknown'

# Get unique users with specific payment methods
users_list = txn_df.filter(
    pl.col('payment_method').is_in([0, 1, 20, 40])
)['user_id'].unique()

# Filter active transactions
txn_df = txn_df.filter(
    (pl.col('user_id').is_in(users_list)) &
    (pl.col('txn_state') == 'active') &
    (pl.col('type_no') == 2)
)

# Calculate next type 2 date
txn_df = txn_df.sort('created').with_columns([
    pl.col('created').shift(-1).over('user_id').cast(pl.Date).alias('next_type_2_date')
])

# Calculate days until next type 2
txn_df = txn_df.with_columns([
   (pl.col('next_type_2_date') - pl.col('created')).dt.total_days().cast(pl.UInt16).alias('days_until_next_type_2')
])

# Add subscription type
txn_df = txn_df.with_columns([
    pl.col('plan_id').map_elements(get_subscription_type, return_dtype=pl.String).alias('current_plan')
])

# Calculate churn
txn_df = txn_df.with_columns([
    pl.when(
        pl.col('next_type_2_date').is_null() |
        (pl.col('next_type_2_date') > pl.col('created').dt.offset_by(churn_month_offset).dt.offset_by(churn_day_offset))
    ).then(1).otherwise(0).cast(pl.Boolean).alias('is_churn')
])

# Use the separate offsets in the filter
txn_df = txn_df.filter(
    pl.col('created')
    .dt.offset_by(churn_month_offset)
    .dt.offset_by(churn_day_offset) <= pl.lit(CUTOFF_DATE)
).sort(['user_id', 'created'])

# Rename columns
txn_df = txn_df.rename({'created': 'created_date'})
# txn_df.head()

# %%
# See example output for user 43
txn_df.filter(pl.col("user_id") == 43)

# %%
# See example output for count of churn for each user
txn_df.group_by('user_id').agg([
    pl.count('is_churn').alias('is_churn_count')
])

# %%
# See the churn count for each subscription type
txn_df.pivot(values="is_churn", index="current_plan", on="is_churn", aggregate_function="len")

# %%
# Select only monthly plan
# print(f"Shape for all plans: {txn_df.shape}")
txn_df = txn_df.filter(pl.col('current_plan') == 'monthly')
# print(f"Shape only for monthly: {txn_df.shape}")

# %% [markdown]
# ## Add user's watch metrics

# %%
def calculate_weekly_metrics(df, user_video_df, log_user_watch_daily_df):
    """
    Calculate weekly metrics for each user-date combination using vectorized operations.
    Each user may have multiple subscription dates, and metrics should be calculated
    relative to each subscription date.
    """
    
    # First, add a row identifier to maintain the relationship with subscription dates
    df = df.with_row_index("row_id")
    
    # Create date ranges for each subscription
    df = df.with_columns([
        pl.col('created_date').cast(pl.Datetime).dt.date().alias('week1_start'),
        (pl.col('created_date').cast(pl.Datetime).dt.date() + pl.duration(days=6)).alias('week1_end'),
        (pl.col('created_date').cast(pl.Datetime).dt.date() + pl.duration(days=7)).alias('week2_start'),
        (pl.col('created_date').cast(pl.Datetime).dt.date() + pl.duration(days=13)).alias('week2_end'),
        (pl.col('created_date').cast(pl.Datetime).dt.date() + pl.duration(days=14)).alias('week3_start'),
        (pl.col('created_date').cast(pl.Datetime).dt.date() + pl.duration(days=20)).alias('week3_end')
    ])

    # Convert bookmarked dates
    user_video_df = user_video_df.with_columns([
        pl.from_epoch('bookmarked').cast(pl.Datetime).dt.date().alias('bookmarked_date')
    ])

    # Function to calculate bookmarked for a specific week
    def calculate_bookmarked(df_dates, user_video_df, start_col, end_col, week_name):
        """
        Calculate bookmarked videos for a specific week.

        Args:
            df_dates: DataFrame with user_id, row_id and date range columns
            user_video_df: DataFrame with user_id and bookmarked_date columns
            start_col: Name of the column containing start date
            end_col: Name of the column containing end date
            week_name: String identifier for the week (e.g., 'week1')
        """
        return (df_dates.join(user_video_df, on='user_id')
                .filter(
                    (pl.col('bookmarked_date') >= pl.col(start_col)) & 
                    (pl.col('bookmarked_date') <= pl.col(end_col))
                )
                .group_by(['user_id', 'row_id'])  # Include row_id to maintain subscription date context
                .agg(pl.len().alias(f'bookmarked_{week_name}')))

    # Function to calculate average playtime in percentage for a specific week
    def calculate_max_playtime_pct(df_dates, watch_logs, start_col, end_col, week_name):
        """
        Calculate average video completion rate in percentage for a specific week.

        Args:
            df_dates: DataFrame with user_id, row_id and date range columns
            watch_logs: DataFrame with watch logs including total_elapsed
            start_col: Name of the column containing start date
            end_col: Name of the column containing end date
            week_name: String identifier for the week (e.g., 'week1')
        """

        return (df_dates.join(
                    watch_logs.with_columns(pl.col('date').cast(pl.Date)), 
                    on='user_id'
                )
                .filter(
                    (pl.col('date') >= pl.col(start_col)) & 
                    (pl.col('date') <= pl.col(end_col))
                )
                .group_by(['user_id', 'row_id'])  # Include row_id to maintain subscription date context
                .agg([
                    pl.col('max_playtime_pct')
                        .mean()
                        .round(2)
                        .alias(f'avg_completion_{week_name}')
                        .cast(pl.Float32)
                ]))
    
    # Function to calculate watch metrics grouped by genre for a specific week
    def calculate_max_playtime_pct_genre(df_dates, watch_logs, start_col, end_col, week_name):
        """
        Calculate average video completion rate in percentage for a specific week grouped by genre.
        
        Args:
            df_dates: DataFrame with user_id, row_id and date range columns
            watch_logs: DataFrame with watch logs including total_elapsed and genre boolean flags
            start_col: Name of the column containing start date
            end_col: Name of the column containing end date
            week_name: String identifier for the week (e.g., 'week1')
            
        Returns:
            DataFrame with watch time in minutes for each genre during the specified week
        """
        genres = ['bl', 'free', 'gay', 'lesbian', 'null', 'queer']
        return (df_dates.join(
                    watch_logs.with_columns(pl.col('date').cast(pl.Date)), 
                    on='user_id'
                )
                .filter(
                    (pl.col('date') >= pl.col(start_col)) & 
                    (pl.col('date') <= pl.col(end_col))
                )
                .group_by(['user_id', 'row_id'])
                .agg([
                    ((pl.col('max_playtime_pct') * pl.col(f'video_genre_{genre}')).mean())
                        .round(2)
                        .alias(f'avg_completion_genre_{genre}_{week_name}')
                        .cast(pl.Float32)
                    for genre in genres
                ]))

    
    # Function to calculate watch metrics grouped by category for a specific week
    def calculate_max_playtime_pct_category(df_dates, watch_logs, start_col, end_col, week_name):
        """
        Calculate average video completion rate in percentage for a specific week grouped by category.
        
        Args:
            df_dates: DataFrame with user_id, row_id and date range columns
            watch_logs: DataFrame with watch logs including max_playtime_pct and category flags
            start_col: Name of the column containing start date
            end_col: Name of the column containing end date
            week_name: String identifier for the week (e.g., 'week1')
            
        Returns:
            DataFrame with watch time in minutes for each category during the specified week
        """
        categories = ['entertainment', 'features', 'null', 'originals', 'series', 'shorts']
        return (df_dates.join(
                    watch_logs.with_columns(pl.col('date').cast(pl.Date)), 
                    on='user_id'
                )
                .filter(
                    (pl.col('date') >= pl.col(start_col)) & 
                    (pl.col('date') <= pl.col(end_col))
                )
                .group_by(['user_id', 'row_id'])
                .agg([
                    ((pl.col('max_playtime_pct') * pl.col(f'video_category_{category}')).mean())
                        .round(2)
                        .alias(f'avg_completion_cat_{category}_{week_name}')
                        .cast(pl.Float32)
                    for category in categories
                ]))

    # Function to calculate watch metrics in minutes for a specific week
    def calculate_watch_minutes(df_dates, watch_logs, start_col, end_col, week_name):
        """
        Calculate watch time in minutes for a specific week.

        Args:
            df_dates: DataFrame with user_id, row_id and date range columns
            watch_logs: DataFrame with watch logs including total_elapsed
            start_col: Name of the column containing start date
            end_col: Name of the column containing end date
            week_name: String identifier for the week (e.g., 'week1')
        """

        return (df_dates.join(
                    watch_logs.with_columns(pl.col('date').cast(pl.Date)), 
                    on='user_id'
                )
                .filter(
                    (pl.col('date') >= pl.col(start_col)) & 
                    (pl.col('date') <= pl.col(end_col))
                )
                .group_by(['user_id', 'row_id'])  # Include row_id to maintain subscription date context
                .agg([
                    (pl.col('total_elapsed').sum() / 60).round(2).alias(f'watch_minutes_{week_name}').cast(pl.Float32)
                ]))
    
    # Function to calculate watch metrics grouped by genre for a specific week
    def calculate_watch_minutes_genre(df_dates, watch_logs, start_col, end_col, week_name):
        """
        Calculate watch time in minutes for a specific week grouped by genre
        
        Args:
            df_dates: DataFrame with user_id, row_id and date range columns
            watch_logs: DataFrame with watch logs including total_elapsed and genre boolean flags
            start_col: Name of the column containing start date
            end_col: Name of the column containing end date
            week_name: String identifier for the week (e.g., 'week1')
            
        Returns:
            DataFrame with watch time in minutes for each genre during the specified week
        """
        genres = ['bl', 'free', 'gay', 'lesbian', 'null', 'queer']
        return (df_dates.join(
                    watch_logs.with_columns(pl.col('date').cast(pl.Date)), 
                    on='user_id'
                )
                .filter(
                    (pl.col('date') >= pl.col(start_col)) & 
                    (pl.col('date') <= pl.col(end_col))
                )
                .group_by(['user_id', 'row_id'])
                .agg([
                    ((pl.col('total_elapsed') * pl.col(f'video_genre_{genre}')).sum() / 60)
                        .round(2)
                        .alias(f'watch_minutes_genre_{genre}_{week_name}')
                        .cast(pl.Float32)
                    for genre in genres
                ]))
    
    # Function to calculate watch metrics grouped by category for a specific week
    def calculate_watch_minutes_category(df_dates, watch_logs, start_col, end_col, week_name):
        """
        Calculate watch time in minutes for a specific week grouped by category
        
        Args:
            df_dates: DataFrame with user_id, row_id and date range columns
            watch_logs: DataFrame with watch logs including total_elapsed and genre boolean flags
            start_col: Name of the column containing start date
            end_col: Name of the column containing end date
            week_name: String identifier for the week (e.g., 'week1')
            
        Returns:
            DataFrame with watch time in minutes for each category during the specified week
        """
        categories = ['entertainment', 'features', 'null', 'originals', 'series', 'shorts']
        return (df_dates.join(
                    watch_logs.with_columns(pl.col('date').cast(pl.Date)), 
                    on='user_id'
                )
                .filter(
                    (pl.col('date') >= pl.col(start_col)) & 
                    (pl.col('date') <= pl.col(end_col))
                )
                .group_by(['user_id', 'row_id'])
                .agg([
                    ((pl.col('total_elapsed') * pl.col(f'video_category_{category}')).sum() / 60)
                        .round(2)
                        .alias(f'watch_minutes_cat_{category}_{week_name}')
                        .cast(pl.Float32)
                    for category in categories
                ]))

    # Calculate metrics for each week
    bookmarked_w1 = calculate_bookmarked(df, user_video_df, 'week1_start', 'week1_end', 'week1')
    bookmarked_w2 = calculate_bookmarked(df, user_video_df, 'week2_start', 'week2_end', 'week2')
    bookmarked_w3 = calculate_bookmarked(df, user_video_df, 'week3_start', 'week3_end', 'week3')
    
    watch_pct_w1 = calculate_max_playtime_pct(df, log_user_watch_daily_df, 'week1_start', 'week1_end', 'week1')
    watch_pct_w2 = calculate_max_playtime_pct(df, log_user_watch_daily_df, 'week2_start', 'week2_end', 'week2')
    watch_pct_w3 = calculate_max_playtime_pct(df, log_user_watch_daily_df, 'week3_start', 'week3_end', 'week3')

    watch_pct_genre_w1 = calculate_max_playtime_pct_genre(df, log_user_watch_daily_df, 'week1_start', 'week1_end', 'week1')
    watch_pct_genre_w2 = calculate_max_playtime_pct_genre(df, log_user_watch_daily_df, 'week2_start', 'week2_end', 'week2')
    watch_pct_genre_w3 = calculate_max_playtime_pct_genre(df, log_user_watch_daily_df, 'week3_start', 'week3_end', 'week3')

    watch_pct_category_w1 = calculate_max_playtime_pct_category(df, log_user_watch_daily_df, 'week1_start', 'week1_end', 'week1')
    watch_pct_category_w2 = calculate_max_playtime_pct_category(df, log_user_watch_daily_df, 'week2_start', 'week2_end', 'week2')
    watch_pct_category_w3 = calculate_max_playtime_pct_category(df, log_user_watch_daily_df, 'week3_start', 'week3_end', 'week3')

    watch_min_w1 = calculate_watch_minutes(df, log_user_watch_daily_df, 'week1_start', 'week1_end', 'week1')
    watch_min_w2 = calculate_watch_minutes(df, log_user_watch_daily_df, 'week2_start', 'week2_end', 'week2')
    watch_min_w3 = calculate_watch_minutes(df, log_user_watch_daily_df, 'week3_start', 'week3_end', 'week3')

    watch_min_genre_w1 = calculate_watch_minutes_genre(df, log_user_watch_daily_df, 'week1_start', 'week1_end', 'week1')
    watch_min_genre_w2 = calculate_watch_minutes_genre(df, log_user_watch_daily_df, 'week2_start', 'week2_end', 'week2')
    watch_min_genre_w3 = calculate_watch_minutes_genre(df, log_user_watch_daily_df, 'week3_start', 'week3_end', 'week3')

    watch_min_category_w1 = calculate_watch_minutes_category(df, log_user_watch_daily_df, 'week1_start', 'week1_end', 'week1')
    watch_min_category_w2 = calculate_watch_minutes_category(df, log_user_watch_daily_df, 'week2_start', 'week2_end', 'week2')
    watch_min_category_w3 = calculate_watch_minutes_category(df, log_user_watch_daily_df, 'week3_start', 'week3_end', 'week3')

    # Join all results using both user_id and row_id to maintain correct relationships
    result = (df.join(bookmarked_w1, on=['user_id', 'row_id'], how='left')
                .join(bookmarked_w2, on=['user_id', 'row_id'], how='left')
                .join(bookmarked_w3, on=['user_id', 'row_id'], how='left')
                .join(watch_pct_w1, on=['user_id', 'row_id'], how='left')
                .join(watch_pct_w2, on=['user_id', 'row_id'], how='left')
                .join(watch_pct_w3, on=['user_id', 'row_id'], how='left')
                .join(watch_pct_genre_w1, on=['user_id', 'row_id'], how='left')
                .join(watch_pct_genre_w2, on=['user_id', 'row_id'], how='left')
                .join(watch_pct_genre_w3, on=['user_id', 'row_id'], how='left')
                .join(watch_pct_category_w1, on=['user_id', 'row_id'], how='left')
                .join(watch_pct_category_w2, on=['user_id', 'row_id'], how='left')
                .join(watch_pct_category_w3, on=['user_id', 'row_id'], how='left')
                .join(watch_min_w1, on=['user_id', 'row_id'], how='left')
                .join(watch_min_w2, on=['user_id', 'row_id'], how='left')
                .join(watch_min_w3, on=['user_id', 'row_id'], how='left')
                .join(watch_min_genre_w1, on=['user_id', 'row_id'], how='left')
                .join(watch_min_genre_w2, on=['user_id', 'row_id'], how='left')
                .join(watch_min_genre_w3, on=['user_id', 'row_id'], how='left')
                .join(watch_min_category_w1, on=['user_id', 'row_id'], how='left')
                .join(watch_min_category_w2, on=['user_id', 'row_id'], how='left')
                .join(watch_min_category_w3, on=['user_id', 'row_id'], how='left')
                )
    
    # Column name modifications
    ## Define base metrics and weeks
    base_metrics = ['bookmarked']
    weeks = ['week1', 'week2', 'week3']

    ## Define genres and categories
    genres = ['bl', 'free', 'gay', 'lesbian', 'null', 'queer']
    categories = ['entertainment', 'features', 'null', 'originals', 'series', 'shorts']

    ## Create column names using list comprehensions
    base_columns = [f"{metric}_{week}" for metric in base_metrics for week in weeks]

    ## Add avg_completion
    completion_base = [f"avg_completion_{week}" for week in weeks]
    completion_genre = [f"avg_completion_genre_{genre}_{week}" for week in weeks for genre in genres]
    completion_category = [f"avg_completion_cat_{category}_{week}" for week in weeks for category in categories]

    ## Add watch minutes
    minutes_base = [f"watch_minutes_{week}" for week in weeks]
    minutes_genre = [f"watch_minutes_genre_{genre}_{week}" for week in weeks for genre in genres]
    minutes_category = [f"watch_minutes_cat_{category}_{week}" for week in weeks for category in categories]

    ## Combine all columns
    all_columns = (base_columns + 
                    completion_base + 
                    completion_genre + 
                    completion_category + 
                    minutes_base +
                    minutes_genre + 
                    minutes_category)

    result = (result
        .with_columns([
            pl.col(all_columns).fill_null(0.0)
        ])
        .drop('row_id')
    )

    return result

# %%
# Add behavior metrics into the txn data by batch | my PC is slow :(

batch_size = 50000
n = txn_df.shape[0]

for i in range(0, n, batch_size):
    batch_df = txn_df.slice(i, batch_size)
    batch_result_df = calculate_weekly_metrics(batch_df, user_video_df, log_user_watch_daily_df)
    
    if i == 0:
        final_result_df = batch_result_df
        # print("Batch 1 processed")
    else:
        final_result_df = pl.concat([final_result_df, batch_result_df])
        # print("Batch", i // batch_size + 1, "processed")

txn_df = final_result_df
# txn_df

# %%
# data_dict = pl.DataFrame({
#     "Column Name": txn_df.columns,
#     "Data Type": [str(dtype) for dtype in txn_df.dtypes]
# })
# data_dict.write_csv("data_dictionary.csv")