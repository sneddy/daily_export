# Direction 1: Learning When to Trust the Crowd

## One-sentence idea

Build a point-in-time model that starts from a prediction-market probability, uses only information available at the same daily cutoff, and learns both how much to adjust the probability and when not to adjust it.

The goal is not to average probabilities from unrelated questions. The goal is to learn whether structured market context contains information that can improve a target market's calibrated forecast.

## Motivation

Prediction markets provide continuously updated probability-like estimates for future binary events. These estimates are useful, but their reliability can vary with the market's uncertainty, activity, time to resolution, topic, and the availability of related information.

A market may be well calibrated when it is liquid and close to resolution, but less reliable when it is thin, new, or weakly connected to other active markets. A useful machine-learning system should therefore do more than predict `yes` or `no`. It should answer two questions:

1. Is the current market probability already trustworthy?
2. If not, is there enough contemporaneous evidence to justify a correction?

This leads to a focused probabilistic forecasting problem rather than a generic price-prediction problem.

## Research question

> Can a model improve the out-of-time calibration of a daily prediction-market forecast by using point-in-time market dynamics and explicitly related markets, while learning when the additional context should be ignored?

The primary prediction target is the final resolved outcome of the target market. The primary input is the market's own daily probability. Additional market context is treated as a possible source of correction, not as a replacement for the market forecast.

## Scope

The initial study uses daily prediction-market data. Each example is a target market observed at a completed daily cutoff before its resolution. The working snapshot currently contains approximately 7,500 filtered markets, 4,500 resolved yes/no markets, and hundreds of thousands of daily observations. These counts describe the current working snapshot and may change when the data are refreshed.

The primary development sample should use all resolved binary markets rather than selecting only the highest-volume markets. The top-volume markets may be used for qualitative examples, but they should not define the main quantitative result.

The main horizon is one day before market close. Three- and seven-day horizons are robustness checks that test whether the same pattern persists when the forecast is made earlier.

## Unit of analysis

Each example represents one target market at one forecast cutoff:

```text
target market
forecast horizon
completed daily observation date
target market state at that date
available context markets at that date
final target outcome
```

The main market probability is:

```text
p_market = price_mean
```

The outcome is:

```text
outcome = 1  if market_result == yes
outcome = 0  if market_result == no
```

Markets with unresolved, scalar, or otherwise ambiguous outcomes are excluded from the primary supervised evaluation.

## Point-in-time data construction

The central engineering requirement is that every feature must be available at the forecast cutoff. The data construction must be auditable from a single timestamp.

### Target cutoff

For a horizon of `h` days:

```text
cutoff_ts = target_close_time - h days
```

The target snapshot is the latest completed daily candle whose `end_period_ts` is no later than `cutoff_ts`.

If a daily candle represents a complete UTC day, it cannot be used before that day has ended. Calendar-date matching is not sufficient on its own; the completed candle timestamp is the authoritative time boundary.

If no admissible target snapshot exists, the example is omitted. A future observation must never be used to fill a missing target snapshot.

### Context retrieval

For the same target cutoff, retrieve context markets that satisfy all of the following conditions:

- the context market existed by the cutoff;
- the context market had not yet closed at the cutoff;
- a completed daily observation is available on the same cutoff date;
- the context market is not the target market;
- the context record contains no resolution outcome or post-cutoff information.

The first implementation should require an exact completed-date match. A later version may allow the most recent earlier observation, but only with an explicit `context_age_days` feature and a strict maximum age.

Context retrieval should be materialized before model training. It should not be re-created implicitly inside a training loop, because the materialized table makes the temporal contract inspectable and reproducible.

## What counts as related context

Relatedness must be represented explicitly. Textual similarity alone does not imply that two probabilities can be combined, and probabilities for different questions must never be averaged by default.

The initial relation policy should use:

1. `same_event` as the primary structural relation;
2. `same_series` as a separate exploratory relation;
3. semantic similarity only as a later ablation;
4. exact logical complements or duplicates only when a relation map has been verified.

Each context market remains an individual input with its own probability and daily features. The model may learn that a context market is useful, irrelevant, or misleading for a particular target. No context probability is converted into a single unconditional consensus probability.

## Available features

### Target-market features

- current `price_mean`;
- probability changes over the previous 1, 3, and 7 days;
- recent volatility;
- `price_high - price_low`;
- daily volume and cumulative volume available up to the cutoff;
- open interest;
- bid–ask spread;
- market age at the cutoff;
- time remaining until close;
- missingness and observation-age indicators.

### Context-market features

- context `price_mean`;
- recent probability changes and volatility;
- daily volume and open interest;
- bid–ask spread;
- time to close;
- relation type;
- optional semantic similarity score;
- context observation age.

### Static information

Question text, event description, series title, and category may be used as static information if they were available when the market was created. Text features should initially be a controlled extension, not a dependency of the core experiment.

### Features that must not be used

The following are not valid point-in-time predictors when calculated over the complete market history:

- final market outcome;
- full-lifetime volume;
- full-lifetime duration;
- first or last observation timestamps;
- final download status;
- post-cutoff market status;
- any aggregate computed using future daily candles;
- outcomes of context markets.

## Model

The model treats the market probability as an anchor rather than an ordinary feature. It predicts a bounded correction and a trust weight:

```text
p_hat = sigmoid(logit(p_market) + trust(x, C) * correction(x, C))
```

where:

- `x` is the target-market state at the cutoff;
- `C` is the set of individually represented context markets;
- `correction` estimates a possible update;
- `trust` determines whether the context should materially affect the forecast.

The context encoder can be a small target-conditioned set encoder or attention module. It must accept a variable number of context markets and preserve relation types. The architecture should remain small enough that the empirical comparison, rather than model scale, explains any improvement.

The model should not be trained to reproduce an average of context probabilities. Its task is to predict the target market's final outcome using the target state and context.

## Experimental ladder

The main comparison should use a small, interpretable ladder:

1. **Global prior:** the training-set fraction of yes outcomes.
2. **Raw market forecast:** `price_mean` without correction.
3. **Static calibration:** logistic or isotonic calibration of `price_mean`.
4. **Target dynamics:** `price_mean` plus the target market's own daily history and activity.
5. **Fixed context features:** target dynamics plus transparent context availability, dispersion, and relation counts.
6. **Learned context model:** target-conditioned encoding of individual context markets.
7. **Shuffled-context control:** the same model with context sets assigned to the wrong targets.

The fixed context condition is a diagnostic baseline, not a claim that context probabilities are directly comparable. It should use descriptive context features rather than an unconditional probability average.

## Evaluation

### Primary metrics

- Brier Score;
- log loss;
- calibration curve and calibration error.

### Secondary metrics

- ROC-AUC;
- risk–coverage curve for selective forecasts;
- performance by forecast horizon;
- performance by volume and uncertainty regime;
- performance by category.

The primary result should compare the raw market probability with the final model probability. A change in the numerical probability is not a success unless it improves out-of-time probabilistic scoring or calibration.

Confidence intervals should be obtained with grouped bootstrap resampling at the event level. Resampling individual daily rows would overstate the effective sample size because rows from the same market are dependent.

## Split protocol

The evaluation must be future-facing:

- training markets resolve earlier than validation markets;
- validation markets resolve earlier than test markets;
- all markets from the same event remain in one split;
- no target market or event identifier is used as a predictive feature;
- no context row is allowed to cross its target cutoff.

Category-held-out evaluation is an additional stress test. The model should be trained on several categories and evaluated on a category not used for training. This tests whether it learns a general reliability pattern rather than category-specific shortcuts.

## Research hypotheses

### H1: Market probabilities are informative

Raw daily market probabilities should outperform simple global or category priors.

### H2: Own-market dynamics add information

Recent movements, activity, and uncertainty should help identify when the current market probability is stale or unstable.

### H3: Related context is conditionally useful

Explicitly related markets should help some target markets, but not uniformly. The benefit should be concentrated in uncertain or low-activity markets.

### H4: Learned trust is better than unconditional context use

A model that can reduce the influence of unreliable context should outperform a model that always applies the same correction.

### H5: The result generalizes

Any improvement should remain visible on later dates, unseen events, and held-out categories.

## Expected contribution

The intended contribution is a machine-learning method and empirical setting for **selective calibration of collective forecasts**:

1. a point-in-time formulation for using daily market state without future leakage;
2. an anchored correction model that treats the market probability as a prior;
3. a relation-aware context mechanism that does not assume probabilities from different questions are directly comparable;
4. an analysis of when collective forecasts are reliable and when additional context helps.

The project is not primarily a new dataset or benchmark release. The data support the experiment, but the paper should stand on the model, the temporal protocol, and the reliability analysis.

## What is deliberately out of scope

The first paper should not depend on:

- five-minute market infrastructure;
- a large time-series foundation model;
- an LLM forecasting competition;
- arbitrary nearest-neighbour probability averaging;
- a model zoo of many unrelated architectures;
- causal claims about how information entered the market;
- convergence and repricing as separate primary tasks.

Those topics may provide later extensions, but they should not obscure the single central question.

## Feasibility gate

Before investing in a complex encoder, run the one-day experiment with all resolved markets:

```text
raw market probability
→ own-market dynamics
→ same-event context
→ shuffled-context control
```

Continue toward a full ML paper only if the context model produces a consistent out-of-time improvement over the target-only model and the improvement survives event-level and category-held-out evaluation.

If context provides no reliable gain, the correct conclusion is that daily market state is already difficult to improve upon. In that case, the project should pivot toward a narrower study of horizon-aware calibration and selective reliability rather than adding architectural complexity.

## Final positioning

The paper should not claim that the model understands all prediction markets or discovers a universal representation of human beliefs. The defensible claim is narrower:

> A point-in-time model can learn when a daily collective forecast should be trusted and when structured contemporaneous market information justifies a calibrated correction.

