from django.core.management.base import BaseCommand, CommandError

from tasq import server

class Command(BaseCommand):
    help = 'Run Server'

    def add_arguments(self, parser):
        # parser.add_argument('poll_id', nargs='+', type=int)
        pass

    def handle(self, *args, **options):
        server.main()

