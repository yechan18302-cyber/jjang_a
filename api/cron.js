/* ── 매분 실행 Push 발송 크론 ──
   cron-job.org 에서 1분마다 이 엔드포인트를 호출합니다.
   Authorization: Bearer {CRON_SECRET}
*/
const { Redis } = require('@upstash/redis');
const redis = new Redis({
    url: process.env.KV_REST_API_URL,
    token: process.env.KV_REST_API_TOKEN,
});
const webpush = require('web-push');

webpush.setVapidDetails(
    'mailto:jjang-scheduler@noreply.com',
    process.env.VAPID_PUBLIC_KEY,
    process.env.VAPID_PRIVATE_KEY
);

function p2(n) { return String(n).padStart(2, '0'); }

module.exports = async function handler(req, res) {
    const auth = req.headers['authorization'];
    if (auth !== `Bearer ${process.env.CRON_SECRET}`) {
        return res.status(401).json({ error: 'Unauthorized' });
    }

    const now = new Date();
    const kst = new Date(now.getTime() + 9 * 60 * 60 * 1000);
    const curTimeStr = `${p2(kst.getUTCHours())}:${p2(kst.getUTCMinutes())}`;
    const todayStr   = kst.toISOString().slice(0, 10);

    try {
        const subKeys = await redis.keys('sub:*');
        let sent = 0, skipped = 0, errors = 0;

        for (const subKey of subKeys) {
            const deviceId = subKey.replace('sub:', '');
            const schedule = await redis.get(`schedule:${deviceId}`);
            if (!schedule || schedule.date !== todayStr) { skipped++; continue; }

            const due = schedule.alarms.filter(a => a.timeStr === curTimeStr && !a.done);
            if (due.length === 0) { skipped++; continue; }

            const subscription = await redis.get(subKey);
            if (!subscription) { skipped++; continue; }

            for (const alarm of due) {
                const sentKey = `sent:${deviceId}:${todayStr}:${alarm.id}`;
                const alreadySent = await redis.get(sentKey);
                if (alreadySent) continue;

                try {
                    await webpush.sendNotification(
                        subscription,
                        JSON.stringify({
                            title: '🐾 짱아 안약 시간!',
                            body: `${alarm.medName} ${alarm.doseLabel} 투약 시간 (${alarm.timeStr})`,
                            icon: '/icon-192.png',
                            badge: '/icon-192.png',
                            tag: `alarm-${alarm.id}`,
                            requireInteraction: true
                        })
                    );
                    await redis.set(sentKey, '1', { ex: 60 * 60 * 24 });
                    sent++;
                } catch (pushErr) {
                    errors++;
                    if (pushErr.statusCode === 410 || pushErr.statusCode === 404) {
                        await redis.del(subKey);
                    }
                }
            }
        }

        return res.status(200).json({ ok: true, time: `${todayStr} ${curTimeStr}`, devices: subKeys.length, sent, skipped, errors });
    } catch (e) {
        return res.status(500).json({ error: e.message });
    }
};
