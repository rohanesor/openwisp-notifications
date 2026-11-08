from asgiref.sync import async_to_sync
from channels import layers
from django.core.cache import cache
from django.db.models import Count, Q
from django.utils.timezone import now, timedelta

from openwisp_notifications.api.serializers import NotFound, NotificationListSerializer
from openwisp_notifications.utils import normalize_unread_count

from .. import settings as app_settings
from ..swapper import load_model

Notification = load_model("Notification")


def notification_state(user):
    """Return the user's notification-storm status and unread count."""
    storm_key = f"ow-noti-storm-{user.pk}"
    in_notification_storm = cache.get(storm_key, False)
    counts = {"unread_count": Count("id", filter=Q(unread=True))}
    if not in_notification_storm:
        timestamp = now()
        counts.update(
            short_term_count=Count(
                "id",
                filter=Q(
                    timestamp__gte=timestamp
                    - timedelta(
                        seconds=app_settings.NOTIFICATION_STORM_PREVENTION[
                            "short_term_time_period"
                        ]
                    )
                ),
            ),
            long_term_count=Count(
                "id",
                filter=Q(
                    timestamp__gte=timestamp
                    - timedelta(
                        seconds=app_settings.NOTIFICATION_STORM_PREVENTION[
                            "long_term_time_period"
                        ]
                    )
                ),
            ),
        )
    notification_counts = user.notifications.aggregate(**counts)
    if not in_notification_storm:
        in_notification_storm = (
            notification_counts["short_term_count"]
            > app_settings.NOTIFICATION_STORM_PREVENTION[
                "short_term_notification_count"
            ]
            or notification_counts["long_term_count"]
            > app_settings.NOTIFICATION_STORM_PREVENTION["long_term_notification_count"]
        )
        if in_notification_storm:
            cache.set(storm_key, True, 60)
    return in_notification_storm, normalize_unread_count(
        notification_counts["unread_count"]
    )


def user_in_notification_storm(user):
    return notification_state(user)[0]


def notification_update_handler(reload_widget=False, notification=None, recipient=None):
    channel_layer = layers.get_channel_layer()
    try:
        assert notification is not None
        notification = NotificationListSerializer(notification).data
    except (NotFound, AssertionError):
        pass
    in_notification_storm, notification_count = notification_state(recipient)
    async_to_sync(channel_layer.group_send)(
        f"ow-notification-{recipient.pk}",
        {
            "type": "send.updates",
            "reload_widget": reload_widget,
            "notification": notification,
            "recipient": str(recipient.pk),
            "in_notification_storm": in_notification_storm,
            "notification_count": notification_count,
        },
    )
