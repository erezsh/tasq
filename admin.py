from django.contrib import admin

from models import Worker, Task

def generate_attr_link(attr, link, title=None):
    def attr_func(self, obj):
        attr_obj = getattr(obj, attr)
        if not attr_obj:
            return ''
        return '<a href="%s">%s</a>' % (link % (attr_obj.id,), attr_obj)
    attr_func.short_description = title or attr.capitalize()
    attr_func.allow_tags = True
    attr_func.admin_order_field = attr
    return attr_func


class TaskAdmin(admin.ModelAdmin):
    list_display = ('func_name', 'start_at', 'queue', 'priority', 'started_at', 'ended_at', 'worker_link', 'status')

    worker_link = generate_attr_link('worker', '/admin/tasq/worker/?id=%s')


admin.site.register(Worker)
admin.site.register(Task, TaskAdmin)

