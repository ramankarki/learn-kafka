import { Controller, Post } from '@nestjs/common';
import { AppService } from './app.service';
import { TOPIC_POST } from './topics';

@Controller('kafka')
export class AppController {
  constructor(private kafkaService: AppService) {}

  @Post('post')
  post() {
    this.kafkaService.addToKafka(TOPIC_POST, {
      title: 'check',
      body: `haneko ${Date.now()}`,
    });
  }
}
