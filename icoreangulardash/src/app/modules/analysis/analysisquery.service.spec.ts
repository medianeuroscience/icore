import { TestBed } from '@angular/core/testing';

import { AnalysisqueryService } from './analysisquery.service';

describe('AnalysisqueryService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: AnalysisqueryService = TestBed.get(AnalysisqueryService);
    expect(service).toBeTruthy();
  });
});
