def get_age_bin_in_text(age):
    try:
        if age == 0: return 'N/A'
        
        if age < 18:
            return 'Under 18'
        elif age >= 18 and age < 30:
            return '18-29'
        elif age >= 30 and age < 40:
            return '30-39'
        elif age >= 40 and age < 50:
            return '40-49'
        elif age >= 50 and age < 60:
            return '50-59'
        elif age >= 50 and age < 70:
            return '60-69'
        elif age >= 50 and age < 80:
            return '70-79'
        else:
            return '80+'
        
    except:
        return 'N/A'


def fix_int(num):
    try:
        return int(float(str(num)))
    except:
        return 0
