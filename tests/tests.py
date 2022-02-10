import util.helpers as h

def pickprof_test():
	assert h.pickprof('D5903649_077.nc') == '077', 'Failed to extract basic profile number'
	assert h.pickprof('D5903649_077D.nc') == '077D', 'Failed to extract decending identifier'

def choose_prefix_test():
	assert h.choose_prefix(['SD', 'SR', 'BD', 'D']) == ['SD'], 'Failed to choose synth delayed option'
	assert h.choose_prefix(['SR', 'BD', 'D']) == ['SR'], 'Failed to choose synth realtime option'
	assert h.choose_prefix(['BD', 'D', 'BR', 'R']) == ['BD', 'D'], 'Failed to choose delayed options'
	assert h.choose_prefix(['D', 'BR', 'R']) == ['BR', 'D'], 'Failed to choose delayed options when available'