using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Reminders.FoundationDb;

public static class SiloBuilderExtensions
{
	public static ISiloBuilder UseFdbReminderService(this ISiloBuilder builder)
	{
		builder.ConfigureServices(services =>
		{
			services.AddReminders();
			services.AddSingleton<IReminderTable, FdbReminderTable>();
		});
		return builder;
	}
}