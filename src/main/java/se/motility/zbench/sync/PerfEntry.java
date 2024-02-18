/*
 * Copyright (c) 2021-2024 Måns Tegling
 *
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.zbench.sync;

import se.motility.zbench.generator.PerfMessage;
import se.motility.ziploq.api.BasicEntry;

public class PerfEntry implements BasicEntry<PerfMessage> {

    private static final long serialVersionUID = 1L;

    private final PerfMessage message;
    private final long businessTs;

    public PerfEntry(PerfMessage message, long businessTs) {
        this.message = message;
        this.businessTs = businessTs;
    }

    @Override
    public PerfMessage getMessage() {
        return message;
    }

    @Override
    public long getBusinessTs() {
        return businessTs;
    }

}
