import unittest

from models.terminal_probability import bachelier_terminal_up_probability, gbm_terminal_up_probability


class TerminalProbabilityTests(unittest.TestCase):
    def test_gbm_probability_is_above_half_when_spot_above_strike(self):
        prob = gbm_terminal_up_probability(
            spot=101.0,
            strike=100.0,
            seconds_to_expiry=30.0,
            annualized_vol=0.8,
        )
        self.assertGreater(prob, 0.5)

    def test_gbm_expired_is_binary(self):
        self.assertEqual(
            gbm_terminal_up_probability(spot=99.0, strike=100.0, seconds_to_expiry=0.0, annualized_vol=0.8),
            0.0,
        )
        self.assertEqual(
            gbm_terminal_up_probability(spot=101.0, strike=100.0, seconds_to_expiry=0.0, annualized_vol=0.8),
            1.0,
        )

    def test_bachelier_probability_uses_expected_noise(self):
        low_noise = bachelier_terminal_up_probability(
            spot=101.0,
            strike=100.0,
            drift_to_expiry=0.0,
            expected_price_std_to_expiry=0.25,
        )
        high_noise = bachelier_terminal_up_probability(
            spot=101.0,
            strike=100.0,
            drift_to_expiry=0.0,
            expected_price_std_to_expiry=5.0,
        )
        self.assertGreater(low_noise, high_noise)


if __name__ == "__main__":
    unittest.main()
