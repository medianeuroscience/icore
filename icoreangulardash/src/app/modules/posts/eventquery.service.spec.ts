import { TestBed } from '@angular/core/testing';

import { EventqueryService } from './eventquery.service';

describe('EventqueryService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: EventqueryService = TestBed.get(EventqueryService);
    expect(service).toBeTruthy();
  });
});
