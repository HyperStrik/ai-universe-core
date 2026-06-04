const {
  BASE_DAILY_CREDITS,
  BONUS_DAILY_CREDITS,
  TRIAL_DAYS,
} = require('../config/constants');

function accountAgeDays(createdAt) {
  const created = new Date(createdAt).getTime();
  return (Date.now() - created) / (1000 * 60 * 60 * 24);
}

function isTrialExpired(user) {
  return user.role === 'CLIENT' && accountAgeDays(user.created_at) > TRIAL_DAYS;
}

function dailyCreditLimit(user) {
  const today = new Date().toISOString().slice(0, 10);
  const bonusDate = user.whatsapp_bonus_awarded_date
    ? String(user.whatsapp_bonus_awarded_date).slice(0, 10)
    : null;
  const bonusActive = bonusDate === today;

  return BASE_DAILY_CREDITS + (bonusActive ? BONUS_DAILY_CREDITS : 0);
}

function hasCreditsRemaining(user) {
  const limit = dailyCreditLimit(user);
  return user.credits_used_today < limit;
}

function bonusAlreadyAwardedToday(user) {
  const today = new Date().toISOString().slice(0, 10);
  const bonusDate = user.whatsapp_bonus_awarded_date
    ? String(user.whatsapp_bonus_awarded_date).slice(0, 10)
    : null;
  return bonusDate === today;
}

module.exports = {
  accountAgeDays,
  isTrialExpired,
  dailyCreditLimit,
  hasCreditsRemaining,
  bonusAlreadyAwardedToday,
};
