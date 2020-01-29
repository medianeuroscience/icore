import { TestBed } from '@angular/core/testing';

import { GkgqueryService } from './gkgquery.service';

describe('GkgqueryService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: GkgqueryService = TestBed.get(GkgqueryService);
    expect(service).toBeTruthy();
  });
});
