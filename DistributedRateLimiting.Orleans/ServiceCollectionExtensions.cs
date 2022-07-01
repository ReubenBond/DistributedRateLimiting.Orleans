using Microsoft.Extensions.DependencyInjection;
using System.Threading.RateLimiting;

namespace DistributedRateLimiting.Orleans
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddDistributedRateLimiter(this IServiceCollection services, Action<DistributedRateLimiterOptions> configureOptions)
        {
            services.AddOptions<DistributedRateLimiterOptions>().Configure(configureOptions);
            services.AddSingleton<RateLimiter, DistributedRateLimiter>();
            return services;
        }
    }
}
