"""The exact discrete quantile, checked against the law it claims to be.

d*P + z*sigma_P asks a continuous symmetric distribution for the quantile of a
count. On this book that count is small -- lambda = d*(R+L) is under 1 unit on
38.1% of ordered lines and under 10 on 86.7% -- so the approximation is being
used exactly where it is weakest.

Measured honestly, the gap turns out to be under a third of a unit at every
lambda, so this is a precision improvement rather than the large correction I
first took it for. It is kept because it is exact and because it removes the
need to rely on pack rounding to rescue the level, not because it moves
service far.
"""
import math

import pytest

from oasis.logic import order_up_to as ou


def _pois_cdf(k, lam):
    s, t = 0.0, math.exp(-lam)
    for i in range(0, int(k) + 1):
        if i:
            t *= lam / i
        s += t
    return s


class TestItIsTheExactQuantile:
    """Not close to Poisson. Equal to it, when the variance says Poisson."""

    @pytest.mark.parametrize("lam", [0.3, 1.0, 1.6, 3.0, 7.5, 20.0])
    @pytest.mark.parametrize("service", [0.90, 0.95, 0.99])
    def test_matches_a_brute_forced_poisson_quantile(self, lam, service):
        P = 10.0
        d = lam / P
        sigma_d = math.sqrt(d)          # sigma_d^2 = d, i.e. pure Poisson
        got = ou.demand_quantile_over(P, d, sigma_d, 0.0, service)
        k = 0
        while _pois_cdf(k, lam) < service:
            k += 1
        assert got == k

    def test_the_result_actually_meets_the_service_it_was_asked_for(self):
        for lam in (0.5, 1.4, 4.0, 11.0):
            P, d = 14.0, lam / 14.0
            s = ou.demand_quantile_over(P, d, math.sqrt(d), 0.0, 0.90)
            assert _pois_cdf(s, lam) >= 0.90
            assert _pois_cdf(s - 1, lam) < 0.90, "not the SMALLEST such level"


class TestOverdispersionUsesNegativeBinomial:
    """Var > mean cannot be Poisson, and must not be silently treated as it."""

    def test_a_higher_phi_never_lowers_the_level(self):
        P, d = 14.0, 0.5
        prev = -1.0
        for phi in (0.0, 0.2, 0.4, 0.8, 1.5, 2.0):
            q = ou.demand_quantile_over(
                P, d, d * ou.demand_cv(d, phi), 0.0, 0.90)
            assert q >= prev
            prev = q

    def test_lead_time_variance_also_raises_it(self):
        P, d = 14.0, 1.0
        sigma_d = d * ou.demand_cv(d)
        a = ou.demand_quantile_over(P, d, sigma_d, 0.0, 0.95)
        b = ou.demand_quantile_over(P, d, sigma_d, 3.0, 0.95)
        assert b > a

    def test_the_matched_moments_are_the_ones_asked_for(self):
        """mu and var of the fitted NB must be the inputs, or the law is not
        the one the cv model describes."""
        for mu, var in ((2.0, 6.0), (5.0, 9.0), (1.0, 4.0)):
            p = mu / var
            r = mu * mu / (var - mu)
            assert r * (1 - p) / p == pytest.approx(mu)
            assert r * (1 - p) / (p * p) == pytest.approx(var)


class TestItAgreesWithTheNormalFormWhereItShould:
    """The threshold has to be justified, not chosen."""

    def test_the_gap_is_sub_unit_once_lambda_is_large(self, monkeypatch):
        monkeypatch.setattr(ou, "DISCRETE_QUANTILE_MAX_LAMBDA", 1e9)
        for lam in (40.0, 80.0, 200.0, 600.0):
            P, d = 20.0, lam / 20.0
            sigma_d = math.sqrt(d)
            sP = ou.demand_sigma_over(P, d, sigma_d, 0.0)
            normal = lam + 1.28 * sP
            exact = ou.demand_quantile_over(P, d, sigma_d, 0.0, 0.8997)
            assert abs(exact - normal) < 1.0, (
                f"lambda={lam}: exact {exact} vs normal {normal:.2f}")

    def test_past_the_threshold_it_returns_the_normal_form(self):
        """Deliberate: walking a distribution with a mean in the thousands one
        unit at a time buys nothing and costs time."""
        P, d = 20.0, 100.0           # lambda = 2000, far past the cutoff
        sigma_d = math.sqrt(d)
        got = ou.demand_quantile_over(P, d, sigma_d, 0.0, 0.8997)
        expect = d * P + ou.z_score(0.8997) * ou.demand_sigma_over(
            P, d, sigma_d, 0.0)
        assert got == pytest.approx(expect, rel=1e-6)


class TestDegenerateInputsAreSafe:
    def test_no_demand_means_no_level(self):
        assert ou.demand_quantile_over(14, 0, 0, 0, 0.9) == 0.0
        assert ou.demand_quantile_over(14, -1, 0, 0, 0.9) == 0.0

    def test_no_horizon_means_no_level(self):
        assert ou.demand_quantile_over(0, 1.0, 1.0, 0, 0.9) == 0.0

    def test_a_zero_service_target_asks_for_nothing(self):
        assert ou.demand_quantile_over(14, 1.0, 1.0, 0, 0.0) == 0.0

    def test_certainty_terminates(self):
        """service = 1 is unreachable for an unbounded count, so the walk must
        stop on its cap rather than spin."""
        v = ou.demand_quantile_over(14, 1.0, 1.0, 0, 1.0)
        assert math.isfinite(v) and v > 0


class TestTheSwitchIsOffUntilItIsChosen:
    def test_order_up_to_level_uses_the_normal_form_by_default(self, monkeypatch):
        monkeypatch.delenv("OASIS_DISCRETE_QUANTILE", raising=False)
        monkeypatch.setattr(ou, "_DISCRETE_QUANTILE", False)
        d, sigma_d = 0.5, 0.5 * ou.demand_cv(0.5)
        got = ou.order_up_to_level(d, sigma_d, 7.0, 7.0, 1.0, 1.28)
        expect = d * 14.0 + 1.28 * ou.demand_sigma_over(14.0, d, sigma_d, 1.0)
        assert got == pytest.approx(expect)

    def test_the_env_switch_selects_the_discrete_level(self, monkeypatch):
        monkeypatch.setenv("OASIS_DISCRETE_QUANTILE", "1")
        d, sigma_d = 0.5, 0.5 * ou.demand_cv(0.5)
        got = ou.order_up_to_level(d, sigma_d, 7.0, 7.0, 1.0, 1.28)
        assert got == float(int(got)), "a discrete level must be a whole count"
        assert got == ou.demand_quantile_over(
            14.0, d, sigma_d, 1.0,
            0.5 * (1.0 + math.erf(1.28 / math.sqrt(2.0))))

    def test_z_is_translated_to_the_service_it_encodes(self, monkeypatch):
        """1.28 must mean 0.90 in both forms, or the switch changes the target
        as well as the method and the comparison is meaningless."""
        monkeypatch.setenv("OASIS_DISCRETE_QUANTILE", "1")
        assert 0.5 * (1.0 + math.erf(1.28 / math.sqrt(2.0))) == pytest.approx(
            0.8997, abs=5e-4)
