using Microsoft.AspNetCore.Mvc;
using WhiteRabbit.Messaging.Abstractions;
using WhiteRabbit.Shared;

namespace WhiteRabbit.Controllers;

[ApiController]
[Route("api/[controller]")]
public class OrdersController(IMessageSender messageSender) : ControllerBase
{
    [HttpPost]
    [ProducesResponseType(StatusCodes.Status202Accepted)]
    public async Task<IActionResult> Post(Order order)
    {
        await messageSender.PublishAsync(order);
        return Accepted();
    }
}