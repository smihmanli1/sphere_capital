
# One day takes 15 mins
# 20 days in a month.
# 500 different runs
# 150000 minutes = 104 days
# Run on 30 cores --> 3.5 days


runCommand=$1


for buyThreshold in 0.05 0.01 0.02 0.03 0.04 0.06 0.07 0.08 0.09 0.10
do
    for sellThreshold in 0 0.01 0.02 0.03 0.04 0.05 0.06 0.07 0.08 0.09
    do
        for positionLimit in 25000 50000 100000 150000 200000
        do
            echo "${runCommand} ${buyThreshold} ${sellThreshold} ${positionLimit} > result_buy_t${buyThreshold}_sell_t${sellThreshold}_pos_limit${positionLimit}.log&"
            ${runCommand} ${buyThreshold} ${sellThreshold} ${positionLimit} > result_buy_t${buyThreshold}_sell_t${sellThreshold}_pos_limit${positionLimit}.log&
        done
    done
done