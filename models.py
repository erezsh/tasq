from django.db import models

Model = models.Model

def apply_kw_override_defaults(field, **defaults):
    def _closure(*args, **kw):
        d = dict(defaults)
        d.update(kw)
        return field(*args, **d)
    return _closure

Str = apply_kw_override_defaults(models.CharField, max_length=255, blank=True, null=False)
Int = apply_kw_override_defaults(models.IntegerField, null=False, blank=True)
BigInt = apply_kw_override_defaults(models.BigIntegerField, null=False, blank=True)
PosInt = apply_kw_override_defaults(models.PositiveIntegerField, null=False, blank=True)
Float = apply_kw_override_defaults(models.FloatField, null=False, blank=True)
Int_Nullable = apply_kw_override_defaults(models.IntegerField, null=True, blank=True)
Bool = apply_kw_override_defaults(models.BooleanField, default=False, null=False, blank=False)
Text = apply_kw_override_defaults(models.TextField, blank=True, null=False)
Date = apply_kw_override_defaults(models.DateTimeField, null=False, blank=True)
ForeignKey = apply_kw_override_defaults(models.ForeignKey, null=False)
M2M = apply_kw_override_defaults(models.ManyToManyField, blank=True)

class Worker(Model):
    STATUS = (
        ('I', 'Initializing'),
        ('i', 'Idle'),
        ('R', 'Running'),
        ('C', 'Closing'),
        ('O', 'Offline'),
    )
    status = Str(choices=STATUS, blank=False, max_length=1, default='I', db_index=True)

    host = Str(blank=False)
    current_task = ForeignKey('Task', null=True, related_name='+')
    tasks_completed = PosInt(default=0)

    started = Date(null=True)

    def __unicode__(self):
        return 'Worker[%s] at %s' % (self.status, self.host)

class Task(Model):
    STATUS = (
        ('I', 'Initializing'),
        ('P', 'Pending'),
        ('R', 'Running'),
        ('S', 'Success'),
        ('E', 'Error'),
        ('C', 'Cancelled'),
        ('p', 'Paused'),
    )
    status = Str(choices=STATUS, blank=False, max_length=1, default='I', db_index=True)

    #### Pending (Setup)
    name = Str(null=True)
    func_module = Str(blank=False)
    func_name = Str(blank=False)
    args = Text()
    start_at = Date(db_index=True)
    time_limit = PosInt(null=True)
    # retries
    queue = Str()
    subtasks = M2M('self', null=True, symmetrical=False, related_name='parent_tasks')
    priority = Int(db_index=True)


    #### Running
    started_at = Date(null=True)
    worker = ForeignKey(Worker, null=True)

    #### Success
    result_str = Text(null=True)
    ended_at = Date(null=True)

    #### Error
    error_str = Text(null=True)

    ### Meta
    created = Date(auto_now_add=True)
    updated = Date(auto_now=True)


    def __unicode__(self):
        return "Task[%s]: %s" % (self.status, self.func_name)
