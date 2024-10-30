select channels, 
    signup_month, 
    sum(contribution_profit_per_signup_halo_effect) as pcp_sum
from tableau.forecast_master
group by channels, signup_month
order by 1, 2